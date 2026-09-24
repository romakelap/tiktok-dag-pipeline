import os
import re
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

MODEL_VERSION = "v1.0"
DB_URL = "mysql+pymysql://airflow_user:AirflowTiktok2026!@localhost:3306/tiktok_oltp?charset=utf8mb4"

def get_engine():
    return create_engine(DB_URL)

# Category tagging rules
RULES = {
    "Edukasi": {
        "keywords": [
            "belajar", "tips", "tutorial", "cara", "edukasi", "informasi", "sejarah", "trik", "rahasia", 
            "fakta", "kelas", "sekolah", "kuliah", "materi", "ilmu", "motivasi", "penjelasan", "pengertian", 
            "mengajar", "guru", "dosen", "buku", "membaca", "wawasan", "paham", "mengerti", "les", "belajaronline", 
            "edukasitiktok", "curiosity", "mindset", "karir", "sukses", "finansial", "sains", "matematika", 
            "fisika", "kimia", "biologi", "bahasa", "inggris", "grammar", "kosakata", "kamus", "definisi"
        ],
        "hashtags": [
            "#belajar", "#tips", "#tutorial", "#cara", "#edukasi", "#informasi", "#sejarah", "#trik", "#fakta", 
            "#ilmu", "#sekolah", "#kuliah", "#motivasi", "#edukasitiktok", "#belajaronline", "#ilmupengetahuan", 
            "#belajaringgris", "#tipspintar", "#caracepat", "#sharinginfo", "#kelas", "#edukatif", "#infomenarik"
        ]
    },
    "Komedi": {
        "keywords": [
            "lucu", "komedi", "ngakak", "kocak", "hiburan", "lawak", "humor", "bercanda", "meme", "parody", 
            "parodi", "prank", "gokil", "ketawa", "receh", "plesetan", "guyonan", "humorist", "konyol", 
            "ketawa_receh", "bodor", "banyolan", "ngocol", "srimulat", "standup", "standupcomedy", "iseng", 
            "ngerjain", "ketawaan", "ngakak_abis", "bengek", "ngakakbrutal", "humorreceh"
        ],
        "hashtags": [
            "#lucu", "#komedi", "#ngakak", "#kocak", "#hiburan", "#lawak", "#humor", "#bercanda", "#parodi", 
            "#parody", "#prank", "#gokil", "#ketawa", "#receh", "#komeditiktok", "#lucu_ngakak", "#ngakakkocak", 
            "#memeindonesia", "#videolucu", "#ketawadong", "#recehbanget", "#ngakakparah", "#standupcomedyindo"
        ]
    },
    "Kuliner": {
        "keywords": [
            "kuliner", "resep", "masak", "makanan", "makan", "minum", "minuman", "mukbang", "jajan", "cemilan", 
            "rasa", "enak", "lezat", "dapur", "koki", "chef", "kulineran", "cafe", "restoran", "warung", "bumbu", 
            "pedas", "manis", "asin", "gurih", "goreng", "rebus", "bakar", "kue", "roti", "bakso", "mie", "nasi", 
            "ayam", "daging", "cokelat", "kopi", "teh", "susu", "sarapan", "lunch", "dinner", "makanmalam", 
            "makansiang", "streetfood", "foodie", "mukbangindonesia"
        ],
        "hashtags": [
            "#kuliner", "#resep", "#masak", "#makanan", "#makan", "#minuman", "#mukbang", "#jajanan", "#cemilan", 
            "#enak", "#kulineran", "#dapur", "#resepmasak", "#makanmakan", "#streetfood", "#foodie", "#resepviral", 
            "#masakansimple", "#jajananviral", "#kulinerindonesia", "#mukbangindo", "#makananenak", "#resepkue"
        ]
    },
    "Teknologi": {
        "keywords": [
            "teknologi", "gadget", "review", "hp", "smartphone", "laptop", "komputer", "software", "aplikasi", 
            "ai", "artificial intelligence", "koding", "programmer", "coding", "tech", "robot", "unboxing", 
            "update", "fitur", "windows", "android", "ios", "iphone", "samsung", "xiaomi", "pc", "hardware", 
            "spesifikasi", "ram", "memori", "chipset", "processor", "keyboard", "mouse", "monitor", "games", 
            "gaming", "playstation", "xbox", "nintendo", "internet", "website", "cyber", "security", "data", "cloud"
        ],
        "hashtags": [
            "#teknologi", "#gadget", "#review", "#hp", "#smartphone", "#laptop", "#komputer", "#software", 
            "#aplikasi", "#ai", "#coding", "#tech", "#unboxing", "#programming", "#robot", "#ios", "#android", 
            "#iphone", "#samsung", "#gaming", "#pcgaming", "#setupgaming", "#developer", "#programmer", "#techreview"
        ]
    },
    "Lifestyle & Home": {
        "keywords": [
            "lifestyle", "home", "rumah", "dekor", "dekorasi", "outfit", "fashion", "style", "ootd", "daily", 
            "vlog", "rutinitas", "a day in my life", "travel", "jalan", "liburan", "belanja", "haul", "skincare", 
            "makeup", "kecantikan", "olahraga", "gym", "aesthetic", "minimalis", "bersih-bersih", "cleaning", 
            "me time", "kopi", "kafe", "kamar", "apartemen", "tanaman", "desain", "baju", "celana", "sepatu", 
            "tas", "aksesoris", "wisata", "hotel", "pantai", "gunung", "diet", "sehat", "yoga", "fitnes", "workout", 
            "rutinitas", "pagi", "sore", "malam"
        ],
        "hashtags": [
            "#lifestyle", "#home", "#rumah", "#dekorasi", "#dekor", "#outfit", "#fashion", "#style", "#ootd", 
            "#dailyvlog", "#adayinmylife", "#travel", "#liburan", "#belanja", "#haul", "#skincare", "#makeup", 
            "#beauty", "#gym", "#olahraga", "#aesthetic", "#minimalis", "#cleaning", "#cozy", "#indotravel", 
            "#workout", "#skincareroutine", "#kamarminimalis", "#aesthetichome", "#ootdhijab", "#fashionstyle"
        ]
    }
}

def clean_and_tokenize(text_str):
    if not text_str or pd.isna(text_str):
        return []
    # Lowercase and find all words
    return re.findall(r"\w+", str(text_str).lower())

def classify_video(title, hashtags, nlp_text):
    # Initialize scores
    scores = {cat: 0.0 for cat in RULES}
    
    title_words = clean_and_tokenize(title)
    nlp_words = clean_and_tokenize(nlp_text)
    
    hashtag_list = []
    if hashtags and not pd.isna(hashtags):
        hashtag_list = [h.strip().lower() for h in re.findall(r"#\w+", str(hashtags))]
        
    for category, rule in RULES.items():
        # Match keywords in title
        for word in title_words:
            if word in rule["keywords"]:
                scores[category] += 1.5
                
        # Match keywords in nlp_text
        for word in nlp_words:
            if word in rule["keywords"]:
                scores[category] += 1.0
                
        # Match hashtags
        for tag in hashtag_list:
            # hashtag list has tags with # sign, rule hashtags also have # sign
            if tag in [r.lower() for r in rule["hashtags"]]:
                scores[category] += 2.0
                
    # Find category with highest score
    max_cat = max(scores, key=scores.get)
    max_score = scores[max_cat]
    
    if max_score == 0:
        # Fallback to Lifestyle & Home with 0.5 confidence
        return "Lifestyle & Home", 0.500000
    
    # Calculate confidence score: base 0.5, plus up to 0.5 based on matches strength
    confidence = min(0.50 + (0.05 * max_score), 1.00)
    return max_cat, round(confidence, 6)

def main():
    print("[START] Seeding video_category_tags table...")
    engine = get_engine()
    
    # Fetch all video records
    sql_fetch = """
        SELECT video_pk, title_full, hashtag_text, nlp_text
        FROM vw_ml_feature_store
    """
    
    print("[DATA] Loading videos from vw_ml_feature_store...")
    df_videos = pd.read_sql(sql_fetch, engine)
    print(f"[DATA] Loaded {len(df_videos)} videos.")
    
    if df_videos.empty:
        print("[WARNING] No videos found in vw_ml_feature_store!")
        return
        
    tagged_records = []
    for _, row in df_videos.iterrows():
        video_pk = int(row["video_pk"])
        title = row["title_full"]
        hashtags = row["hashtag_text"]
        nlp_text = row["nlp_text"]
        
        category, confidence = classify_video(title, hashtags, nlp_text)
        tagged_records.append({
            "video_pk": video_pk,
            "core_category": category,
            "confidence_score": confidence,
            "tagging_method": "rule_based",
            "model_version": MODEL_VERSION
        })
        
    print(f"[PROCESS] Classifications done. Preparing to save {len(tagged_records)} records...")
    
    # Truncate rule_based tagging records or delete all from video_category_tags first
    with engine.begin() as conn:
        print("[DB] Deleting old rule-based tag records...")
        conn.execute(text("DELETE FROM video_category_tags WHERE tagging_method = 'rule_based'"))
        
        # Batch insert to avoid huge single transaction/query
        sql_insert = text("""
            INSERT INTO video_category_tags (
                video_pk, core_category, confidence_score, tagging_method, model_version
            ) VALUES (
                :video_pk, :core_category, :confidence_score, :tagging_method, :model_version
            )
        """)
        
        # Bulk execute
        conn.execute(sql_insert, tagged_records)
        print("[DB] Inserted all records successfully.")
        
    print("[SUCCESS] Rule-based category seeding completed.")

if __name__ == "__main__":
    main()
