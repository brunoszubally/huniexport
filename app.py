from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import requests
from datetime import datetime
import os
from dotenv import load_dotenv
from typing import List, Optional
from pydantic import BaseModel
from fastapi.security import APIKeyHeader

# Környezeti változók betöltése
load_dotenv()

app = FastAPI(title="Huniexport API")

# Adalo API konfiguráció (környezeti változóból)
ADALO_APP_ID = "78abf0f7-0d48-492e-98b5-ee301ebe700e"
ADALO_COLLECTION_ID = "t_e11t5tqgg6jbkbq4a1z596kqt"
ADALO_API_KEY = os.getenv("ADALO_API_KEY") # Visszaállítjuk a környezeti változót

# Saját API kulcs az autentikációhoz (környezeti változóból)
API_KEY = os.getenv("SERVICE_API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key")

def get_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Érvénytelen API kulcs")
    return api_key

class Transaction(BaseModel):
    id: int
    transaction_id: str
    transaction_status: str
    user_transaction: List[int]
    partner_transaction: List[int]
    coupon_transaction: List[int]
    spend_value: int
    discount_value: int
    saved_value: int
    hunicoin_value: int
    jouser_transact: List[int]
    test_user_transaction: str
    created_at: str
    updated_at: str

class GetTransactionsRequest(BaseModel):
    partner_id: int
    # Adalo custom function-ök gyakran küldenek más adatokat is, 
    # ha kellenek, itt add hozzá őket (pl. other_data: Any)


@app.post("/get-partner-transactions")
async def get_partner_transactions(
    request_data: GetTransactionsRequest,
    api_key: str = Depends(get_api_key) # Autentikáció hozzáadása
):
    """
    Lekéri egy partner összes 'finalized' tranzakcióját és JSON-ként visszaadja
    """
    partner_id = request_data.partner_id

    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (SERVICE_API_KEY környezeti változó)")

    print(f"\n=== Új kérés kezdése partner_id={partner_id} ===")
    
    # Adalo API hívás
    url = f"https://api.adalo.com/v0/apps/{ADALO_APP_ID}/collections/{ADALO_COLLECTION_ID}"
    headers = {
        "Authorization": f"Bearer {ADALO_API_KEY}",
        "Content-Type": "application/json"
    }
    
    print(f"API URL: {url}")
    print(f"API Headers: {headers}")
    
    try:
        print("Adalo API hívás indítása...")
        response = requests.get(url, headers=headers)
        print(f"Adalo API válasz státuszkód: {response.status_code}")
        print(f"Adalo API válasz fejlécek: {dict(response.headers)}")
        
        if response.status_code != 200:
            print(f"Hibás Adalo API válasz: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Adalo API hiba: {response.text}"
            )
        
        print("Adalo API válasz feldolgozása...")
        print(f"Válasz tartalom (első 1000 karakter): {response.text[:1000]}")
        
        try:
            transactions = response.json()
            print(f"JSON válasz típusa: {type(transactions)}")
            
            # Adalo API válasz formátum ellenőrzése
            if isinstance(transactions, dict):
                if "records" in transactions:
                    transactions = transactions["records"]
                    print(f"Talált records lista hossza: {len(transactions)}")
                #else:
                    # Adalo API néha közvetlenül a listát adja vissza dictionary nélkül
                    #print(f"Hiányzó 'records' kulcs az Adalo API válaszból. Elérhető kulcsok: {list(transactions.keys())}")
                    #raise HTTPException(
                    #    status_code=500,
                    #    detail=f"Hiányzó 'records' kulcs az Adalo API válaszból. Elérhető kulcsok: {list(transactions.keys())}"
                    #)
            elif not isinstance(transactions, list):
                raise HTTPException(
                    status_code=500,
                    detail=f"Váratlan Adalo API válasz formátum: {type(transactions)}"
                )
            
            print(f"Összes Adalo tranzakció száma: {len(transactions)}")
            
            # Szűrés partner ID és státusz alapján
            finalized_partner_transactions = []
            for t in transactions:
                if isinstance(t, dict):
                    #print(f"Tranzakció kulcsok: {list(t.keys())}") # Túl sok log lehet
                    if t.get("transaction_status") == "finalized":
                         if "partner_transaction" in t:
                            if partner_id in t["partner_transaction"]:
                                finalized_partner_transactions.append(t)
            
            print(f"Talált 'finalized' partner tranzakciók száma: {len(finalized_partner_transactions)}")
            
            if not finalized_partner_transactions:
                # 200-as státusz, de üres lista, ha nincs találat (Adalo custom function friendly)
                 return JSONResponse(content=[], status_code=200)
                # raise HTTPException(
                #    status_code=404,
                #    detail=f"Nem található 'finalized' tranzakció a partner_id={partner_id} számára"
                # )
            
            # JSON válasz visszaadása
            # DataFrame használata az adatok átalakításához és tisztításához lehet hasznos
            # de most közvetlenül a listát adjuk vissza, ha nincs extra feldolgozás
            # df = pd.DataFrame(finalized_partner_transactions)
            # return JSONResponse(content=df.to_dict(orient='records'), status_code=200)

            return JSONResponse(content=finalized_partner_transactions, status_code=200)
            
        except ValueError as e:
            print(f"JSON feldolgozási hiba: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Hibás JSON válasz az Adalo API-tól: {str(e)}"
            )
        
    except requests.exceptions.RequestException as e:
        print(f"Adalo API hívási hiba: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Hiba az Adalo API hívás során: {str(e)}"
        )
    except Exception as e:
         print(f"Váratlan hiba történt: {str(e)}")
         raise HTTPException(status_code=500, detail=f"Váratlan szerverhiba: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    # Ezt a blokkot csak lokális fejlesztéshez használjuk.
    # Koyeb/Render más módon indítja el az alkalmazást (pl. gunicorn vagy uvicorn)
    # Ügyelj rá, hogy a SERVICE_API_KEY és ADALO_API_KEY környezeti változók be legyenek állítva lokálisan (.env)
    # és a telepítési platformon is.
    uvicorn.run(app, host="0.0.0.0", port=8000) 