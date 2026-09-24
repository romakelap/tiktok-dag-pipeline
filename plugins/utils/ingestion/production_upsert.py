"""
Production UPSERT — pindahkan data dari staging ke production tables.

Flow per table:
1. videos_echotik (UPSERT)
2. hashtags_echotik (UPSERT)
3. video_metrics_snapshot (APPEND only, time-series)
4. influencers (UPSERT — extract dari video records)
5. regions (UPSERT — extract dari hashtag records)
6. BI summary refresh

All in 1 transaction (atomic).
"""
import logging
from typing import Dict
import json
import ast

from utils.parsers.echotik_parser import (
    parse_count,
    parse_money_local,
    parse_money_usd,
)


def derive_follower_tier(num: int) -> str:
    """Derive follower tier dari count"""
    if num is None or num <= 0:
        return None
    if num < 1_000:
        return 'nano'
    elif num < 10_000:
        return 'micro'
    elif num < 100_000:
        return 'mid'
    elif num < 1_000_000:
        return 'macro'
    else:
        return 'mega'


def derive_revenue_tier(gmv: float) -> str:
    """Derive revenue tier dari GMV USD"""
    if gmv is None or gmv == 0:
        return 'no_sales'
    if gmv < 100:
        return 'low'
    elif gmv < 1_000:
        return 'medium'
    elif gmv < 10_000:
        return 'high'
    else:
        return 'top'


def upsert_regions(db_hook, run_id: str) -> Dict[str, int]:
    """
    Insert/Update regions dari hashtag staging table.
    Region info ada di hashtag (region_id sebagai code, region_name).
    """
    # Insert regions yang belum ada
    inserted = db_hook.execute("""
        INSERT INTO regions (region_code, region_name, region_key)
        SELECT DISTINCT 
            region_id,
            COALESCE(region_name, region_id),
            LOWER(region_id)
        FROM hashtags_echotik_staging
        WHERE ingest_run_id = :run_id
          AND region_id IS NOT NULL 
          AND region_id != ''
          AND is_valid = 1
          AND region_id NOT IN (SELECT region_code FROM regions)
    """, params={'run_id': run_id})
    
    count = inserted.rowcount if inserted else 0
    logging.info(f"Regions: {count} inserted")
    return {'inserted': count, 'updated': 0}


def upsert_influencers(db_hook, run_id: str) -> Dict[str, int]:
    """
    Insert/Update influencers dari videos staging table.
    """
    # Get pre-counts
    before_count = db_hook.count_rows("influencers")
    
    # UPSERT: insert influencer baru, update yang sudah ada
    db_hook.execute("""
        INSERT INTO influencers (
            influencer_id, unique_id, display_name,
            avatar_url,
            follower_count_raw, follower_count_num, follower_tier,
            sales_flag, region_id
        )
        SELECT DISTINCT
            influencer_id,
            COALESCE(NULLIF(influencer_unique_id, ''), CONCAT('unknown_', influencer_id)) AS unique_id,
            COALESCE(NULLIF(influencer_name, ''), 'Unknown') AS display_name,
            NULLIF(influencer_avatar_url, '') AS avatar_url,
            follower_count_raw,
            follower_count_num,
            CASE
                WHEN follower_count_num >= 1000000 THEN 'mega'
                WHEN follower_count_num >= 100000 THEN 'macro'
                WHEN follower_count_num >= 10000 THEN 'mid'
                WHEN follower_count_num >= 1000 THEN 'micro'
                WHEN follower_count_num > 0 THEN 'nano'
                ELSE NULL
            END AS follower_tier,
            COALESCE(sales_flag, 0),
            (SELECT region_id FROM regions WHERE region_code = videos_echotik_staging.influencer_region LIMIT 1)
        FROM videos_echotik_staging
        WHERE ingest_run_id = :run_id
          AND is_valid = 1
          AND influencer_id IS NOT NULL
          AND influencer_id > 0
        ON DUPLICATE KEY UPDATE
            display_name   = VALUES(display_name),
            -- Only overwrite avatar_url when the new value is non-null
            -- so a later run without avatar data never clears a good URL.
            avatar_url     = COALESCE(VALUES(avatar_url), avatar_url),
            follower_count_raw = VALUES(follower_count_raw),
            follower_count_num = VALUES(follower_count_num),
            follower_tier  = VALUES(follower_tier),
            sales_flag     = VALUES(sales_flag),
            last_fetched_at = CURRENT_TIMESTAMP
    """, params={'run_id': run_id})
    
    after_count = db_hook.count_rows("influencers")
    
    inserted = after_count - before_count
    
    # Affected total (insert + update) — approximate via touched rows
    touched = db_hook.query_scalar("""
        SELECT COUNT(DISTINCT influencer_id) 
        FROM videos_echotik_staging 
        WHERE ingest_run_id = :run_id AND is_valid = 1 AND influencer_id > 0
    """, params={'run_id': run_id}) or 0
    
    updated = max(0, touched - inserted)
    
    logging.info(f"Influencers: {inserted} inserted, {updated} updated")
    return {'inserted': inserted, 'updated': updated}


def upsert_videos(db_hook, run_id: str) -> Dict[str, int]:
    """
    UPSERT videos dari staging ke videos_echotik production.
    """
    before_count = db_hook.count_rows("videos_echotik")
    
    db_hook.execute("""
        INSERT INTO videos_echotik (
            echotik_video_id, data_source,
            title_full, title_brief,
            cover_url, video_url,
            influencer_id,
            duration_raw, duration_seconds, duration_bucket,
            is_ai_video, is_promote, is_latest, is_delete,
            published_at,
            title_length, hashtag_count, mention_count, emoji_count, has_question,
            published_day_of_week, published_hour
        )
        SELECT 
            echotik_video_id,
            data_source,
            title_full,
            LEFT(COALESCE(title_brief, LEFT(title_full, 255)), 255),
            cover_url, video_url,
            influencer_id,
            duration_raw, duration_seconds, duration_bucket,
            COALESCE(is_ai_video, 0),
            COALESCE(is_promote, 0),
            COALESCE(is_latest, 0),
            COALESCE(is_delete, 0),
            published_at,
            title_length,
            COALESCE(hashtag_count, 0),
            COALESCE(mention_count, 0),
            COALESCE(emoji_count, 0),
            COALESCE(has_question, 0),
            DAYOFWEEK(published_at) - 1,
            HOUR(published_at)
        FROM videos_echotik_staging
        WHERE ingest_run_id = :run_id
          AND is_valid = 1
          AND influencer_id IS NOT NULL
          AND influencer_id > 0
        ON DUPLICATE KEY UPDATE
            title_full = VALUES(title_full),
            title_brief = VALUES(title_brief),
            cover_url = VALUES(cover_url),
            video_url = VALUES(video_url),
            duration_raw = VALUES(duration_raw),
            duration_seconds = VALUES(duration_seconds),
            duration_bucket = VALUES(duration_bucket),
            is_ai_video = VALUES(is_ai_video),
            is_promote = VALUES(is_promote),
            is_latest = VALUES(is_latest),
            data_source = VALUES(data_source),
            last_fetched_at = CURRENT_TIMESTAMP
    """, params={'run_id': run_id})
    
    after_count = db_hook.count_rows("videos_echotik")
    inserted = after_count - before_count
    
    touched = db_hook.query_scalar("""
        SELECT COUNT(*) FROM videos_echotik_staging
        WHERE ingest_run_id = :run_id AND is_valid = 1 AND influencer_id > 0
    """, params={'run_id': run_id}) or 0
    
    updated = max(0, touched - inserted)
    
    logging.info(f"Videos: {inserted} inserted, {updated} updated")
    return {'inserted': inserted, 'updated': updated}



def upsert_products_and_links(db, run_id: str, selling_records: list) -> dict:
    result = {'products': 0, 'video_products': 0}

    for record in selling_records:
        video_id = record.get('echotik_video_id', '')
        if not video_id:
            continue

        # Parse products_json
        raw_json = record.get('products_json', '[]')
        try:
            if isinstance(raw_json, str):
                products = json.loads(raw_json)
            else:
                products = raw_json or []
        except Exception:
            logging.warning(f"Failed to parse products_json for video {video_id}")
            continue

        if not products:
            continue

        for idx, p in enumerate(products):
            product_id = str(p.get('product_id', '')).strip()
            if not product_id:
                continue

            avg_local, currency = parse_money_local(p.get('avg_price', '0'))
            gmv_local, _ = parse_money_local(p.get('total_gmv_amt', '0'))

            # UPSERT products — sekarang semua field lengkap
            db.execute("""
                INSERT INTO products (
                    echotik_product_id, product_name, category_name_raw,
                    cover_url, real_price_num,
                    avg_price_raw, avg_price_local, avg_price_currency,
                    avg_price_fz_raw, avg_price_usd,
                    total_sale_cnt_num,
                    total_gmv_amt_raw, total_gmv_amt_local,
                    total_gmv_amt_fz_raw, total_gmv_amt_usd,
                    last_fetched_at
                ) VALUES (
                    :pid, :name, :cat, :cover, :real_price,
                    :avg_raw, :avg_local, :currency,
                    :avg_fz_raw, :avg_usd,
                    :sale_cnt,
                    :gmv_raw, :gmv_local,
                    :gmv_fz_raw, :gmv_usd,
                    NOW()
                )
                ON DUPLICATE KEY UPDATE
                    product_name        = VALUES(product_name),
                    total_sale_cnt_num  = VALUES(total_sale_cnt_num),
                    total_gmv_amt_usd   = VALUES(total_gmv_amt_usd),
                    total_gmv_amt_local = VALUES(total_gmv_amt_local),
                    avg_price_usd       = VALUES(avg_price_usd),
                    last_fetched_at     = NOW()
            """, params={
                'pid': product_id,
                'name': p.get('product_name', ''),
                'cat': p.get('category_name', ''),
                'cover': p.get('cover_url', ''),
                'real_price': parse_count(p.get('real_price', '0')),
                'avg_raw': p.get('avg_price', ''),
                'avg_local': avg_local,
                'currency': currency,
                'avg_fz_raw': p.get('avg_price_fz', ''),
                'avg_usd': parse_money_usd(p.get('avg_price_fz', '$0')),
                'sale_cnt': parse_count(p.get('total_sale_cnt', '0')),
                'gmv_raw': p.get('total_gmv_amt', ''),
                'gmv_local': gmv_local,
                'gmv_fz_raw': p.get('total_gmv_amt_fz', ''),
                'gmv_usd': parse_money_usd(p.get('total_gmv_amt_fz', '$0')),
            })
            result['products'] += 1

            # UPSERT video_products
            db.execute("""
                INSERT INTO video_products (
                    video_pk, product_pk,
                    video_sale_cnt_num,
                    video_gmv_amt_local, video_gmv_amt_usd,
                    position_in_video, last_updated_at
                )
                SELECT
                    v.video_pk, p.product_pk,
                    :sale_cnt, :gmv_local, :gmv_usd,
                    :position, NOW()
                FROM videos_echotik v
                JOIN products p ON p.echotik_product_id = :pid
                WHERE v.echotik_video_id = :vid
                ON DUPLICATE KEY UPDATE
                    video_sale_cnt_num  = VALUES(video_sale_cnt_num),
                    video_gmv_amt_usd   = VALUES(video_gmv_amt_usd),
                    last_updated_at     = NOW()
            """, params={
                'vid': video_id,
                'pid': product_id,
                'sale_cnt': parse_count(p.get('video_sale_cnt', '0')),
                'gmv_local': parse_money_local(p.get('video_gmv_amt', '0'))[0],
                'gmv_usd': parse_money_usd(p.get('video_gmv_amt_fz', '$0')),
                'position': idx + 1,
            })
            result['video_products'] += 1

    logging.info(f"Products upserted: {result}")
    return result

def upsert_hashtags(db_hook, run_id: str) -> Dict[str, int]:
    """UPSERT hashtags."""
    before_count = db_hook.count_rows("hashtags_echotik")
    
    db_hook.execute("""
        INSERT INTO hashtags_echotik (
            echotik_tag_id, tag_title, tag_title_brief,
            region_id,
            video_count_raw, video_count_num,
            views_count_raw, views_count_num,
            likes_count_num, comments_count_num,
            shares_count_num, favorites_count_num,
            avg_views_per_video, competition_level
        )
        SELECT 
            echotik_tag_id,
            tag_title,
            LEFT(COALESCE(tag_title_brief, tag_title), 100),
            (SELECT region_id FROM regions WHERE region_code = s.region_id LIMIT 1),
            video_count_raw, video_count_num,
            views_count_raw, views_count_num,
            likes_count_num, comments_count_num,
            shares_count_num, favorites_count_num,
            avg_views_per_video, competition_level
        FROM hashtags_echotik_staging s
        WHERE ingest_run_id = :run_id
          AND is_valid = 1
        ON DUPLICATE KEY UPDATE
            tag_title = VALUES(tag_title),
            tag_title_brief = VALUES(tag_title_brief),
            video_count_raw = VALUES(video_count_raw),
            video_count_num = VALUES(video_count_num),
            views_count_raw = VALUES(views_count_raw),
            views_count_num = VALUES(views_count_num),
            likes_count_num = VALUES(likes_count_num),
            avg_views_per_video = VALUES(avg_views_per_video),
            competition_level = VALUES(competition_level),
            last_fetched_at = CURRENT_TIMESTAMP
    """, params={'run_id': run_id})
    
    after_count = db_hook.count_rows("hashtags_echotik")
    inserted = after_count - before_count
    
    touched = db_hook.query_scalar("""
        SELECT COUNT(*) FROM hashtags_echotik_staging
        WHERE ingest_run_id = :run_id AND is_valid = 1
    """, params={'run_id': run_id}) or 0
    
    updated = max(0, touched - inserted)
    
    logging.info(f"Hashtags: {inserted} inserted, {updated} updated")
    return {'inserted': inserted, 'updated': updated}


def insert_video_metrics_snapshots(db_hook, run_id: str) -> Dict[str, int]:
    """
    APPEND-ONLY insert ke video_metrics_snapshot.
    Time-series: tiap ingest = snapshot baru.
    """
    db_hook.execute("""
        INSERT INTO video_metrics_snapshot (
            video_pk,
            views_raw, views_num,
            likes_num, comments_num, shares_num,
            likes_per_views_raw, likes_per_views_num,
            engagement_rate_raw, engagement_rate_num,
            sales_count,
            gmv_usd,
            gmv_raw_local, gmv_local, gmv_local_currency,
            snapshot_at
        )
        SELECT 
            v.video_pk,
            s.views_raw, s.views_num,
            s.likes_num, s.comments_num, s.shares_num,
            s.likes_per_views_raw, s.likes_per_views_num,
            s.engagement_rate_raw, s.engagement_rate_num,
            COALESCE(s.sales_count, s.total_sale_cnt_num, 0),
            COALESCE(s.gmv_usd, s.total_gmv_amt_usd, 0),
            NULL,
            s.gmv_local,
            s.gmv_local_currency,
            NOW()
        FROM video_metrics_snapshot_staging s
        JOIN videos_echotik v ON s.echotik_video_id = v.echotik_video_id
        WHERE s.ingest_run_id = :run_id
          AND s.is_valid = 1
    """, params={'run_id': run_id})
    
    count = db_hook.query_scalar("""
        SELECT COUNT(*) FROM video_metrics_snapshot_staging s
        JOIN videos_echotik v ON s.echotik_video_id = v.echotik_video_id
        WHERE s.ingest_run_id = :run_id AND s.is_valid = 1
    """, params={'run_id': run_id}) or 0
    
    logging.info(f"Snapshots: {count} inserted (time-series)")
    return {'inserted': int(count), 'updated': 0}


def refresh_bi_video_summary(db_hook) -> int:
    """Refresh bi_video_revenue_summary dengan latest snapshot."""
    
    # Clear & rebuild
    db_hook.execute("TRUNCATE TABLE bi_video_revenue_summary")
    
    db_hook.execute("""
                INSERT INTO bi_video_revenue_summary (
                    video_pk, influencer_id,
                    latest_views, latest_likes,
                    latest_sales, latest_gmv_usd,
                    latest_gmv_local, latest_gmv_currency,
                    revenue_tier
                )
                SELECT 
                    v.video_pk,
                    v.influencer_id,
                    latest.views_num,
                    latest.likes_num,
                    latest.sales_count,
                    latest.gmv_usd,
                    latest.gmv_local,
                    latest.gmv_local_currency,
                    CASE 
                        WHEN latest.gmv_usd >= 10000 THEN 'top'
                        WHEN latest.gmv_usd >= 1000 THEN 'high'
                        WHEN latest.gmv_usd >= 100 THEN 'medium'
                        WHEN latest.gmv_usd > 0 THEN 'low'
                        ELSE 'no_sales'
                    END
                FROM videos_echotik v
                JOIN (
                    SELECT s1.*
                    FROM video_metrics_snapshot s1
                    INNER JOIN (
                        SELECT video_pk, MAX(snapshot_at) AS max_time
                        FROM video_metrics_snapshot
                        GROUP BY video_pk
                    ) s2 ON s1.video_pk = s2.video_pk AND s1.snapshot_at = s2.max_time
                ) latest ON v.video_pk = latest.video_pk
                ON DUPLICATE KEY UPDATE
                    latest_views    = VALUES(latest_views),
                    latest_likes    = VALUES(latest_likes),
                    latest_sales    = VALUES(latest_sales),
                    latest_gmv_usd  = VALUES(latest_gmv_usd),
                    latest_gmv_local = VALUES(latest_gmv_local),
                    latest_gmv_currency = VALUES(latest_gmv_currency),
                    revenue_tier    = VALUES(revenue_tier)
    """)
    
    count = db_hook.count_rows("bi_video_revenue_summary")
    logging.info(f"BI video summary refreshed: {count} rows")
    return count


def refresh_bi_hashtag_summary(db_hook) -> int:
    """Refresh bi_hashtag_revenue_summary."""
    
    db_hook.execute("TRUNCATE TABLE bi_hashtag_revenue_summary")
    
    # Note: butuh video_hashtags_echotik untuk proper aggregation
    # Untuk sekarang, basic aggregate dari hashtag stats sendiri
    db_hook.execute("""
        INSERT INTO bi_hashtag_revenue_summary (
            hashtag_pk,
            total_videos, total_views, total_likes,
            avg_views_per_video, avg_engagement_rate,
            total_sales, total_gmv_usd
        )
        SELECT 
            h.hashtag_pk,
            COALESCE(h.video_count_num, 0),
            COALESCE(h.views_count_num, 0),
            COALESCE(h.likes_count_num, 0),
            h.avg_views_per_video,
            CASE 
                WHEN h.views_count_num > 0 THEN 
                    LEAST((COALESCE(h.likes_count_num, 0) + COALESCE(h.comments_count_num, 0) + COALESCE(h.shares_count_num, 0)) 
                    / h.views_count_num, 99.9999)
                ELSE 0
            END,
            0,
            0
        FROM hashtags_echotik h
    """)
    
    count = db_hook.count_rows("bi_hashtag_revenue_summary")
    logging.info(f"BI hashtag summary refreshed: {count} rows")
    return count


def upsert_video_categories(db_hook, run_id: str) -> Dict[str, int]:
    """
    UPSERT video category tags dari videos_echotik_staging ke video_category_tags.
    """
    inserted = db_hook.execute("""
        INSERT INTO video_category_tags (
            video_pk, core_category, confidence_score, tagging_method, model_version
        )
        SELECT 
            v.video_pk,
            s.category_name,
            1.000000 AS confidence_score,
            'ml_classifier' AS tagging_method,
            'v1.0' AS model_version
        FROM videos_echotik_staging s
        JOIN videos_echotik v ON s.echotik_video_id = v.echotik_video_id
        WHERE s.ingest_run_id = :run_id
          AND s.is_valid = 1
          AND s.category_name IS NOT NULL
          AND s.category_name != ''
        ON DUPLICATE KEY UPDATE
            core_category = VALUES(core_category),
            confidence_score = VALUES(confidence_score),
            tagging_method = VALUES(tagging_method),
            model_version = VALUES(model_version),
            updated_at = CURRENT_TIMESTAMP
    """, params={'run_id': run_id})
    
    count = inserted.rowcount if inserted else 0
    logging.info(f"Video Category Tags: {count} upserted")
    return {'inserted': count, 'updated': 0}

