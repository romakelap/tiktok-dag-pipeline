"""
Category Tagging Rules for Ingestion Pipeline & ML Classifier.
Defines extensive domain keywords, hashtags, and n-grams for 5 core categories.
"""

CATEGORY_RULES = {
    "Edukasi": {
        "keywords": [
            "belajar", "tips", "tutorial", "cara", "edukasi", "informasi", "sejarah", "trik", "rahasia", 
            "fakta", "kelas", "sekolah", "kuliah", "materi", "ilmu", "motivasi", "penjelasan", "pengertian", 
            "mengajar", "guru", "dosen", "buku", "membaca", "wawasan", "paham", "mengerti", "les", "belajaronline", 
            "edukasitiktok", "curiosity", "mindset", "karir", "sukses", "finansial", "sains", "matematika", 
            "fisika", "kimia", "biologi", "bahasa", "inggris", "grammar", "kosakata", "kamus", "definisi",
            "psikologi", "mental", "bedah", "analisis", "pengetahuan", "panduan", "stepbystep", "strategi",
            "solusi", "rumus", "ujian", "beasiswa", "mahasiswa", "pelajar", "skripsi", "tesis", "tutor",
            "investasi", "saham", "crypto", "bisnis", "marketing", "tipsandtricks", "diy", "creative",
            "study", "studygram", "productivity", "produktif", "selfimprovement", "publicspeaking", "presentasi"
        ],
        "hashtags": [
            "#belajar", "#tips", "#tutorial", "#cara", "#edukasi", "#informasi", "#sejarah", "#trik", "#fakta", 
            "#ilmu", "#sekolah", "#kuliah", "#motivasi", "#edukasitiktok", "#belajaronline", "#ilmupengetahuan", 
            "#belajaringgris", "#tipspintar", "#caracepat", "#sharinginfo", "#kelas", "#edukatif", "#infomenarik",
            "#serunyabelajar", "#samasamabelajar", "#tipsbisnis", "#tipsmarketing", "#belajarsaham", "#selfimprovement",
            "#studytok", "#studytips", "#infopenting", "#serunyaberbagi", "#faktaunik", "#tipskerja", "#tipskarir"
        ]
    },
    "Komedi": {
        "keywords": [
            "lucu", "komedi", "ngakak", "kocak", "hiburan", "lawak", "humor", "bercanda", "meme", "parody", 
            "parodi", "prank", "gokil", "ketawa", "receh", "plesetan", "guyonan", "humorist", "konyol", 
            "ketawa_receh", "bodor", "banyolan", "ngocol", "srimulat", "standup", "standupcomedy", "iseng", 
            "ngerjain", "ketawaan", "ngakak_abis", "bengek", "ngakakbrutal", "humorreceh", "komedian",
            "tebak", "tebakan", "garing", "lucubanget", "lucutiktok", "ngakakk", "lucubet", "kelakuan",
            "koplak", "lawakan", "acting", "skit", "drama", "pov", "relate", "parodiindo", "komediindo",
            "ngelawak", "becanda", "roasting", "dubbing", "funnymoments", "joke", "jokes", "lol", "lmao"
        ],
        "hashtags": [
            "#lucu", "#komedi", "#ngakak", "#kocak", "#hiburan", "#lawak", "#humor", "#bercanda", "#parodi", 
            "#parody", "#prank", "#gokil", "#ketawa", "#receh", "#komeditiktok", "#lucu_ngakak", "#ngakakkocak", 
            "#memeindonesia", "#videolucu", "#ketawadong", "#recehbanget", "#ngakakparah", "#standupcomedyindo",
            "#komedireceh", "#videolucubanget", "#ngakak_unik", "#lucuabis", "#humortiktok", "#dagelan", "#ngakakbareng",
            "#serunyadihibur", "#hiburanlucu", "#relateable", "#pov", "#drama"
        ]
    },
    "Kuliner": {
        "keywords": [
            "kuliner", "resep", "masak", "makanan", "makan", "minum", "minuman", "mukbang", "jajan", "cemilan", 
            "rasa", "enak", "lezat", "dapur", "koki", "chef", "kulineran", "cafe", "restoran", "warung", "bumbu", 
            "pedas", "manis", "asin", "gurih", "goreng", "rebus", "bakar", "kue", "roti", "bakso", "mie", "nasi", 
            "ayam", "daging", "cokelat", "kopi", "teh", "susu", "sarapan", "lunch", "dinner", "makanmalam", 
            "makansiang", "streetfood", "foodie", "mukbangindonesia", "cireng", "seblak", "kari", "sambal",
            "pedes", "crispy", "renyah", "boba", "icecream", "eskrim", "snack", "coffeetime", "ngopi",
            "streetfoodindonesia", "dessert", "foodvlog", "foodreview", "makanenak", "cemilankekinian",
            "dimsum", "soto", "rendang", "sate", "bebek", "ikan", "seafood", "jus", "sirup", "keju", "cheese",
            "pizza", "burger", "pasta", "cooking", "resepmasakan", "resepsimple", "baking", "pastry"
        ],
        "hashtags": [
            "#kuliner", "#resep", "#masak", "#makanan", "#makan", "#minuman", "#mukbang", "#jajanan", "#cemilan", 
            "#enak", "#kulineran", "#dapur", "#resepmasak", "#makanmakan", "#streetfood", "#foodie", "#resepviral", 
            "#masakansimple", "#jajananviral", "#kulinerindonesia", "#mukbangindo", "#makananenak", "#resepkue",
            "#serunyakuliner", "#kulineria", "#foodtiktok", "#resepnusantara", "#kulinerjakarta", "#kulinersurabaya",
            "#kulinerbandung", "#makanterus", "#racunkuliner", "#reseptiktok", "#reviewmakanan", "#makanankekinian"
        ]
    },
    "Teknologi": {
        "keywords": [
            "teknologi", "gadget", "review", "hp", "smartphone", "laptop", "komputer", "software", "aplikasi", 
            "ai", "artificial intelligence", "koding", "programmer", "coding", "tech", "robot", "unboxing", 
            "update", "fitur", "windows", "android", "ios", "iphone", "samsung", "xiaomi", "pc", "hardware", 
            "spesifikasi", "ram", "memori", "chipset", "processor", "keyboard", "mouse", "monitor", "games", 
            "gaming", "playstation", "xbox", "nintendo", "internet", "website", "cyber", "security", "data", "cloud",
            "qwen", "deepseek", "chatgpt", "openai", "aitools", "aitool", "macbook", "ipad", "tablet",
            "smartwatch", "tws", "earphone", "headset", "camera", "kamera", "drone", "apple", "google",
            "oppo", "vivo", "realme", "infinix", "asus", "rog", "lenovo", "acer", "developer", "codinglife",
            "webdev", "python", "javascript", "react", "html", "css", "machinelearning", "crypto", "blockchain",
            "metaverse", "techreview", "unboxingvideo", "gadgetindonesia", "spesifikasihp", "hpgaming"
        ],
        "hashtags": [
            "#teknologi", "#gadget", "#review", "#hp", "#smartphone", "#laptop", "#komputer", "#software", 
            "#aplikasi", "#ai", "#coding", "#tech", "#unboxing", "#programming", "#robot", "#ios", "#android", 
            "#iphone", "#samsung", "#gaming", "#pcgaming", "#setupgaming", "#developer", "#programmer", "#techreview",
            "#serunyateknologi", "#gadgetreview", "#techtok", "#techtips", "#aitools", "#qwen", "#chatgpt",
            "#hpmurah", "#hpgaming", "#reviewhp", "#reviewgadget", "#unboxinghp", "#infogadget", "#smartphonereview"
        ]
    },
    "Lifestyle & Home": {
        "keywords": [
            "lifestyle", "home", "rumah", "dekor", "dekorasi", "outfit", "fashion", "style", "ootd", "daily", 
            "vlog", "rutinitas", "a day in my life", "travel", "jalan", "liburan", "belanja", "haul", "skincare", 
            "makeup", "kecantikan", "olahraga", "gym", "aesthetic", "minimalis", "bersih-bersih", "cleaning", 
            "me time", "kopi", "kafe", "kamar", "apartemen", "tanaman", "desain", "baju", "celana", "sepatu", 
            "tas", "aksesoris", "wisata", "hotel", "pantai", "gunung", "diet", "sehat", "yoga", "fitnes", "workout", 
            "rutinitas", "pagi", "sore", "malam", "parfum", "perfume", "hijab", "gamis", "pashmina", "dress",
            "skincareroutine", "beauty", "glowing", "serum", "sunscreen", "moisturizer", "liptint", "cushion",
            "bedak", "lipstik", "facial", "salon", "barbershop", "haircut", "rambut", "kerudung", "ootdindo",
            "homedecor", "roommakeover", "interiordesign", "perabot", "furnitur", "peralatanrumah", "sprei",
            "kasur", "daster", "dompet", "perhiasan", "kalung", "cincin", "gelang", "jamtangan", "staycation",
            "healing", "healingtime", "traveling", "pesona_indonesia", "wisataindonesia", "ootdhijab", "racunshopee",
            "racuntiktok", "keranjang_kuning", "keranjangkuning", "unboxinghaul", "shopeehaul"
        ],
        "hashtags": [
            "#lifestyle", "#home", "#rumah", "#dekorasi", "#dekor", "#outfit", "#fashion", "#style", "#ootd", 
            "#dailyvlog", "#adayinmylife", "#travel", "#liburan", "#belanja", "#haul", "#skincare", "#makeup", 
            "#beauty", "#gym", "#olahraga", "#aesthetic", "#minimalis", "#cleaning", "#cozy", "#indotravel", 
            "#workout", "#skincareroutine", "#kamarminimalis", "#aesthetichome", "#ootdhijab", "#fashionstyle",
            "#racunskincare", "#makeuptutorial", "#beautytips", "#homedecor", "#roommakeover", "#staycation",
            "#healing", "#traveltiktok", "#outfitideas", "#fashiontiktok", "#racuntiktok", "#keranjangkuning"
        ]
    }
}
