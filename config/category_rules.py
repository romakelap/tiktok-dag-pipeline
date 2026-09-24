"""
Category Tagging Rules for Ingestion Pipeline.
Matches classify_rules.py in the ML Service.
"""

CATEGORY_RULES = {
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
