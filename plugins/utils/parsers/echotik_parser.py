"""
Parser utilities — convert raw Echotik response ke standardized format.

Handle:
- String number ("61.81M") → integer (61810000)
- Percentage ("2.75%") → decimal (0.0275)
- Money ("$28,900", "Rp1.3B") → numeric
- Duration ("37s") → seconds
- Datetime string → ISO format
"""
import re
import logging
from typing import Optional, Union, List, Dict
from datetime import datetime
import json


def parse_count(raw: Union[str, int, None]) -> int:
    """
    Convert string number dengan suffix ke integer.
    Examples:
        "61.81M" → 61_810_000
        "1.7K" → 1_700
        "2.5B" → 2_500_000_000
        "100" → 100
    """
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        return int(raw)
    
    raw = str(raw).strip()
    if not raw:
        return 0
    
    try:
        if 'M' in raw:
            return int(float(raw.replace('M', '').replace(',', '')) * 1_000_000)
        if 'K' in raw:
            return int(float(raw.replace('K', '').replace(',', '')) * 1_000)
        if 'B' in raw:
            return int(float(raw.replace('B', '').replace(',', '')) * 1_000_000_000)
        return int(float(raw.replace(',', '')))
    except (ValueError, TypeError):
        logging.warning(f"Failed to parse count: {raw}")
        return 0


def parse_percentage(raw: Union[str, float, None]) -> float:
    """
    Convert percentage string ke decimal.
    Examples:
        "2.75%" → 0.0275
        "0.59%" → 0.0059
        "100%" → 1.0
    """
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    
    raw = str(raw).strip()
    if not raw:
        return 0.0
    
    try:
        return float(raw.replace('%', '').replace(',', '')) / 100
    except (ValueError, TypeError):
        return 0.0


def parse_money_usd(raw: Union[str, None]) -> float:
    """
    Convert USD money string ke float.
    Examples:
        "$28,900" → 28900.00
        "$0" → 0.0
        "$67.1K" → 67100.0
        "$503.98K" → 503980.0
    """
    if raw is None:
        return 0.0
    
    raw = str(raw).strip().replace('$', '').replace(',', '').replace('\\uffe5', '')
    if not raw or raw == '0':
        return 0.0
    
    try:
        if 'K' in raw:
            return float(raw.replace('K', '')) * 1_000
        if 'M' in raw:
            return float(raw.replace('M', '')) * 1_000_000
        if 'B' in raw:
            return float(raw.replace('B', '')) * 1_000_000_000
        return float(raw)
    except (ValueError, TypeError):
        return 0.0


def parse_money_local(raw: Union[str, None]) -> tuple:
    """
    Convert local currency money string. Return (amount, currency).
    Examples:
        "Rp1.3B" → (1_300_000_000, "IDR")
        "Rp172.68M" → (172_680_000, "IDR")
        "¥12.46" → (12.46, "JPY")
    """
    if raw is None:
        return 0.0, "UNKNOWN"
    
    raw = str(raw).strip()
    if not raw or raw == '0':
        return 0.0, "UNKNOWN"
    
    # Detect currency
    currency = "UNKNOWN"
    if 'Rp' in raw:
        currency = "IDR"
        raw = raw.replace('Rp', '')
    elif '¥' in raw or '\\uffe5' in raw:
        currency = "JPY"
        raw = raw.replace('¥', '').replace('\\uffe5', '')
    elif '$' in raw:
        currency = "USD"
        raw = raw.replace('$', '')
    
    raw = raw.replace(',', '').strip()
    
    try:
        if 'K' in raw:
            return float(raw.replace('K', '')) * 1_000, currency
        if 'M' in raw:
            return float(raw.replace('M', '')) * 1_000_000, currency
        if 'B' in raw:
            return float(raw.replace('B', '')) * 1_000_000_000, currency
        return float(raw), currency
    except (ValueError, TypeError):
        return 0.0, currency


def parse_duration(raw: Union[str, int, None]) -> int:
    """
    Convert duration string ke seconds.
    Examples:
        "37s" → 37
        "1m30s" → 90
        "15" → 15
    """
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        return int(raw)
    
    raw = str(raw).strip()
    if not raw:
        return 0
    
    total_seconds = 0
    
    # Pattern: extract minutes and seconds
    min_match = re.search(r'(\d+)m', raw)
    sec_match = re.search(r'(\d+)s', raw)
    
    if min_match:
        total_seconds += int(min_match.group(1)) * 60
    if sec_match:
        total_seconds += int(sec_match.group(1))
    
    if total_seconds == 0:
        # Try direct number
        try:
            total_seconds = int(raw.replace('s', '').replace('m', ''))
        except ValueError:
            return 0
    
    return total_seconds


def parse_datetime(raw: Union[str, None]) -> Optional[str]:
    """
    Parse datetime string ke ISO format.
    Examples:
        "2026-04-14 19:19:29" → "2026-04-14T19:19:29"
        "2026-05-04" → "2026-05-04T00:00:00"
    """
    if not raw:
        return None
    
    raw = str(raw).strip()
    
    # Try common formats
    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    
    logging.warning(f"Failed to parse datetime: {raw}")
    return None


def extract_hashtags(text: str) -> List[str]:
    """Extract hashtag dari title text"""
    if not text:
        return []
    return [tag[1:] for tag in re.findall(r'#\w+', text)]


def extract_mentions(text: str) -> List[str]:
    """Extract mention (@user) dari title text"""
    if not text:
        return []
    return [m[1:] for m in re.findall(r'@\w+', text)]


def count_emoji(text: str) -> int:
    """Count emoji di text (simple unicode range detection)"""
    if not text:
        return 0
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002600-\U000026FF"  # misc symbols
        "\U00002700-\U000027BF"  # dingbats
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FA6F"
        "]+",
        flags=re.UNICODE
    )
    return len(emoji_pattern.findall(text))


# ============================================
# MAIN PARSER FUNCTIONS PER API TYPE
# ============================================

def parse_video_library_record(raw: Dict, category_name: str) -> Dict:
    """
    Parse 1 record dari API Video Library ke standardized format.
    """
    title = raw.get('video_title', '')
    influencer = raw.get('influencer', {}) or {}
    
    parsed = {
        # Identifiers
        'echotik_video_id': raw.get('video_id', ''),
        'influencer_id': raw.get('influencer_id', ''),
        'data_source': 'library',
        'category_name': category_name,
        
        # Title & content
        'title_full': title,
        'title_brief': raw.get('video_title_brief', ''),
        'cover_url': raw.get('cover_url', ''),
        'video_url': raw.get('video_url', ''),
        
        # Influencer info
        'influencer_name': raw.get('influencer_name', ''),
        'influencer_unique_id': influencer.get('unique_id', ''),
        'influencer_avatar_url': influencer.get('avatar_url', ''),  # TikTok profile picture from Echotik CDN
        'follower_count_raw': influencer.get('follower_count', ''),
        'follower_count_num': parse_count(influencer.get('follower_count', '0')),
        'influencer_region': (influencer.get('region') or {}).get('id', ''),
        'sales_flag': int(influencer.get('sales_flag', 0)),

        # Engagement metrics
        'duration_raw': raw.get('duration', ''),
        'duration_seconds': parse_duration(raw.get('duration', '')),
        'views_num': parse_count(raw.get('views_count', raw.get('play_count', '0'))),
        'likes_num': parse_count(raw.get('digg_count', '0')),
        'comments_num': parse_count(raw.get('comment_count', '0')),
        'shares_num': parse_count(raw.get('share_count', '0')),
        'engagement_rate_raw': raw.get('engagement_rate', ''),
        'engagement_rate_num': parse_percentage(raw.get('engagement_rate', '0%')),
        'likes_per_views_raw': raw.get('likes_per_views', ''),
        'likes_per_views_num': parse_percentage(raw.get('likes_per_views', '0%')),
        
        # Sales (biasanya 0 di library endpoint)
        'sales_count': parse_count(raw.get('sales', '0')),
        'gmv_usd': parse_money_usd(raw.get('gmv', '$0')),
        
        # Flags
        'is_ai_video': int(raw.get('is_ai_video', 0)),
        'is_promote': bool(raw.get('is_promote', False)),
        'is_latest': int(raw.get('is_latest', 0)),
        'is_delete': int(raw.get('is_delete', 0)),
        
        # Dates
        'published_at': parse_datetime(raw.get('publish_time', '')),
        'updated_at_echotik': parse_datetime(raw.get('update_time', '')),
        
        # Computed features (enrichment)
        'title_length': len(title),
        'hashtag_count': len(extract_hashtags(title)),
        'mention_count': len(extract_mentions(title)),
        'emoji_count': count_emoji(title),
        'has_question': '?' in title,
        'extracted_hashtags': extract_hashtags(title),
        'extracted_mentions': extract_mentions(title),
        
        # Audit
        'fetched_at': datetime.utcnow().isoformat(),
    }
    
    # Compute duration bucket
    duration = parsed['duration_seconds']
    if duration < 15:
        parsed['duration_bucket'] = 'very_short'
    elif duration < 30:
        parsed['duration_bucket'] = 'short'
    elif duration < 60:
        parsed['duration_bucket'] = 'medium'
    else:
        parsed['duration_bucket'] = 'long'
    
    # Compute viral score (weighted formula)
    parsed['viral_score'] = (
        parsed['views_num'] * 0.3 +
        parsed['likes_num'] * 0.25 +
        parsed['shares_num'] * 0.25 +
        parsed['comments_num'] * 0.2
    ) / 100
    
    # Engagement tier
    eng = parsed['engagement_rate_num']
    if eng >= 0.15:
        parsed['engagement_tier'] = 'viral'
    elif eng >= 0.10:
        parsed['engagement_tier'] = 'high'
    elif eng >= 0.05:
        parsed['engagement_tier'] = 'medium'
    else:
        parsed['engagement_tier'] = 'low'
    
    return parsed


def parse_hashtag_record(raw: Dict) -> Dict:
    """Parse 1 record dari API Hashtag Library"""
    region = raw.get('region', {}) or {}
    
    views = parse_count(raw.get('views_count', '0'))
    video_count = parse_count(raw.get('video_count', '0'))
    
    parsed = {
        'echotik_tag_id': raw.get('tag_id', ''),
        'tag_title': raw.get('tag_title', ''),
        'tag_title_brief': raw.get('tag_title_brief', ''),
        'region_id': region.get('id', ''),
        'region_name': region.get('name', ''),
        
        # Raw + parsed counts
        'video_count_raw': raw.get('video_count', ''),
        'video_count_num': video_count,
        'views_count_raw': raw.get('views_count', ''),
        'views_count_num': views,
        'likes_count_num': parse_count(raw.get('digg_count', '0')),
        'comments_count_num': parse_count(raw.get('comment_count', '0')),
        'shares_count_num': parse_count(raw.get('share_count', '0')),
        'favorites_count_num': parse_count(raw.get('favorite_count', '0')),
        
        # Computed metrics
        'avg_views_per_video': views / video_count if video_count > 0 else 0,
        
        # Audit
        'fetched_at': datetime.utcnow().isoformat(),
    }
    
    # Competition level based on video count
    if video_count >= 10_000_000:
        parsed['competition_level'] = 'extreme'
    elif video_count >= 1_000_000:
        parsed['competition_level'] = 'high'
    elif video_count >= 100_000:
        parsed['competition_level'] = 'medium'
    else:
        parsed['competition_level'] = 'low'
    
    return parsed


def parse_video_selling_record(raw: Dict, category_name: str) -> Dict:
    """Parse 1 record dari API Video Selling (leaderboard)"""
    influencer = raw.get('influencer', {}) or {}
    products = raw.get('video_products', []) or []
    
    # GMV & sales
    total_gmv_local, currency = parse_money_local(raw.get('total_gmv_amt', '0'))
    total_gmv_usd = parse_money_usd(raw.get('total_gmv_amt_fz', '$0'))
    
    parsed = {
        # Identifiers
        'echotik_video_id': raw.get('video_id', ''),
        'influencer_id': raw.get('influencer_id', ''),
        'data_source': 'shop',
        'category_name': category_name,
        
        # Title & content
        'title_full': raw.get('video_title', ''),
        'title_brief': raw.get('video_title_brief', ''),
        'cover_url': raw.get('cover_url', ''),
        'video_url': raw.get('video_url', ''),
        
        # Influencer info
        'influencer_name': raw.get('influencer_name', ''),
        'influencer_unique_id': influencer.get('unique_id', ''),
        'influencer_nick_name': influencer.get('nick_name', ''),
        'influencer_avatar_url': influencer.get('avatar_url', ''),  # TikTok profile picture from Echotik CDN
        'follower_count_raw': influencer.get('follower_count', ''),
        'follower_count_num': parse_count(influencer.get('follower_count', '0')),
        'influencer_region': (influencer.get('region') or {}).get('id', ''),
        
        # Engagement metrics (per video)
        'duration_raw': raw.get('duration', ''),
        'duration_seconds': parse_duration(raw.get('duration', '')),
        'views_num': parse_count(raw.get('views_count', '0')),
        'likes_num': parse_count(raw.get('digg_count', '0')),
        'comments_num': parse_count(raw.get('comment_count', '0')),
        'shares_num': parse_count(raw.get('share_count', '0')),
        'interact_ratio_raw': raw.get('interact_ratio', ''),
        'interact_ratio_num': parse_percentage(raw.get('interact_ratio', '0%')),
        
        # Sales & GMV
        'total_sale_cnt_raw': raw.get('total_sale_cnt', ''),
        'total_sale_cnt_num': parse_count(raw.get('total_sale_cnt', '0')),
        'total_gmv_amt_raw': raw.get('total_gmv_amt', ''),
        'total_gmv_amt_local': total_gmv_local,
        'total_gmv_amt_currency': currency,
        'total_gmv_amt_fz_raw': raw.get('total_gmv_amt_fz', ''),
        'total_gmv_amt_usd': total_gmv_usd,
        
        # Accumulated stats
        'total_views_count_raw': raw.get('total_views_count', ''),
        'total_views_count_num': parse_count(raw.get('total_views_count', '0')),
        'total_digg_count_num': parse_count(raw.get('total_digg_count', '0')),
        'total_comment_count_num': parse_count(raw.get('total_comment_count', '0')),
        'total_share_count_num': parse_count(raw.get('total_share_count', '0')),
        
        # Products
        'products_count': len(products),
        'product_ids': [p.get('product_id', '') for p in products],
        'product_names': [p.get('product_name', '') for p in products],
        'products_json': json.dumps(products), 
        # Dates
        'published_at': parse_datetime(raw.get('publish_time', '')),
        
        # Audit
        'fetched_at': datetime.utcnow().isoformat(),
    }
    
    # Revenue tier
    if total_gmv_usd >= 100_000:
        parsed['revenue_tier'] = 'top'
    elif total_gmv_usd >= 10_000:
        parsed['revenue_tier'] = 'high'
    elif total_gmv_usd >= 1_000:
        parsed['revenue_tier'] = 'medium'
    elif total_gmv_usd > 0:
        parsed['revenue_tier'] = 'low'
    else:
        parsed['revenue_tier'] = 'no_sales'
    
    return parsed


def parse_product_record(raw: Dict, video_id: str) -> Dict:
    """Parse 1 product dari video_products array"""
    avg_price_local, currency = parse_money_local(raw.get('avg_price', '0'))
    total_gmv_local, _ = parse_money_local(raw.get('total_gmv_amt', '0'))
    
    return {
        'echotik_product_id': raw.get('product_id', ''),
        'video_id': video_id,
        'product_name': raw.get('product_name', ''),
        'category_name_raw': raw.get('category_name', ''),
        'cover_url': raw.get('cover_url', ''),
        
        'real_price_raw': raw.get('real_price', ''),
        'real_price_num': parse_count(raw.get('real_price', '0')),
        
        'avg_price_raw': raw.get('avg_price', ''),
        'avg_price_local': avg_price_local,
        'avg_price_currency': currency,
        'avg_price_fz_raw': raw.get('avg_price_fz', ''),
        'avg_price_usd': parse_money_usd(raw.get('avg_price_fz', '$0')),
        
        'total_sale_cnt_raw': raw.get('total_sale_cnt', ''),
        'total_sale_cnt_num': parse_count(raw.get('total_sale_cnt', '0')),
        'total_gmv_amt_raw': raw.get('total_gmv_amt', ''),
        'total_gmv_amt_local': total_gmv_local,
        'total_gmv_amt_fz_raw': raw.get('total_gmv_amt_fz', ''),
        'total_gmv_amt_usd': parse_money_usd(raw.get('total_gmv_amt_fz', '$0')),
        
        'video_sale_cnt_num': parse_count(raw.get('video_sale_cnt', '0')),
        'video_gmv_amt_usd': parse_money_usd(raw.get('video_gmv_amt_fz', '$0')),
        
        'fetched_at': datetime.utcnow().isoformat(),
    }
