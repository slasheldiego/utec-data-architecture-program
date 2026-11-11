# Toma 1.000 reseñas reales de Philadelphia que NO están en la muestra (las que siguen a la 400.000)
import json, gzip
SRC, CIUDAD, SALTAR, TOMAR = "./yelp_dataset", "philadelphia", 400_000, 1_000

negocios = set()
with open(f"{SRC}/yelp_academic_dataset_business.json", encoding="utf8") as f:
    for linea in f:
        b = json.loads(linea)
        if b.get("city", "").strip().lower() == CIUDAD:
            negocios.add(b["business_id"])

vistas = tomadas = 0
with open(f"{SRC}/yelp_academic_dataset_review.json", encoding="utf8") as f, \
     gzip.open("review_2.json.gz", "wt", encoding="utf8") as g:
    for linea in f:
        if json.loads(linea)["business_id"] in negocios:
            vistas += 1
            if vistas > SALTAR:
                g.write(linea)
                tomadas += 1
                if tomadas >= TOMAR:
                    break
print(f"✔ review_2.json.gz con {tomadas} reseñas nuevas")
