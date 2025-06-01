# Huniexport API

Ez a szolgáltatás lehetővé teszi az Adalo tranzakciók exportálását Excel formátumban egy adott partner számára.

## Telepítés

1. Klónozd le a repót:
```bash
git clone https://github.com/yourusername/huniexport.git
cd huniexport
```

2. Hozz létre egy virtuális környezetet és aktiváld:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# vagy
.\venv\Scripts\activate  # Windows
```

3. Telepítsd a függőségeket:
```bash
pip install -r requirements.txt
```

4. Hozz létre egy `.env` fájlt és add meg az Adalo API kulcsot:
```
ADALO_API_KEY=your_api_key_here
```

## Használat

1. Indítsd el a szervert:
```bash
python app.py
```

2. A szolgáltatás elérhető lesz a `http://localhost:8000` címen.

3. A tranzakciók lekéréséhez használd a következő végpontot:
```
GET /transactions/{partner_id}
```

Példa:
```bash
curl http://localhost:8000/transactions/151
```

A válasz egy Excel fájl lesz, ami tartalmazza az adott partner összes tranzakcióját.

## API Dokumentáció

A teljes API dokumentáció elérhető a `http://localhost:8000/docs` címen a Swagger UI segítségével. 