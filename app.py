from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Request
from fastapi.responses import JSONResponse, FileResponse
import pandas as pd
import requests
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
from typing import List, Optional
from pydantic import BaseModel

# Környezeti változók betöltése
load_dotenv()

# --- ADALO KONFIGURÁCIÓK (csak itt, mindenhol ezekre hivatkozunk) ---
# Users (felhasználók) collection
ADALO_USERS_APP_ID = os.getenv("ADALO_USERS_APP_ID", "78abf0f7-0d48-492e-98b5-ee301ebe700e")
ADALO_USERS_COLLECTION_ID = os.getenv("ADALO_USERS_COLLECTION_ID", "t_0013b9f2134b4b79b0820993b01145d4")

# Transactions (tranzakciók) collection
ADALO_TRANSACTIONS_APP_ID = os.getenv("ADALO_TRANSACTIONS_APP_ID", "105b8ea3-f2e9-498e-b939-03d445237d78")
ADALO_TRANSACTIONS_COLLECTION_ID = os.getenv("ADALO_TRANSACTIONS_COLLECTION_ID", "t_e11t5tqgg6jbkbq4a1z596kqt")

# Statisztika collection
ADALO_STATS_APP_ID = os.getenv("ADALO_STATS_APP_ID", "105b8ea3-f2e9-498e-b939-03d445237d78")
ADALO_STATS_COLLECTION_ID = os.getenv("ADALO_STATS_COLLECTION_ID", "t_ashzitr0lvm0u1dibo7jada75")

# Coupons (kuponok) collection
ADALO_COUPONS_APP_ID = os.getenv("ADALO_COUPONS_APP_ID", "105b8ea3-f2e9-498e-b939-03d445237d78")
ADALO_COUPONS_COLLECTION_ID = os.getenv("ADALO_COUPONS_COLLECTION_ID", "t_4qtjjg01audh5q46q83lc7rkt")

# Fő API kulcs (mindenhez ugyanaz, ha nincs külön)
ADALO_API_KEY = os.getenv("ADALO_API_KEY", "2oq7qmxcjwa4m1tcqdf1w1e8i")

# Transactions API kulcs (külön, mert a tranzakciók másik app-ban vannak)
ADALO_TRANSACTIONS_API_KEY = os.getenv("ADALO_TRANSACTIONS_API_KEY", "2f7hg3qfd2fctfrf3argfal9d")

app = FastAPI(title="Huniexport API")

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

class SendMailsRequest(BaseModel):
    template_id: str
    subject: str
    from_email: str = "info@nextfoto.hu"
    from_name: str = "Huniversity"
    # Opcionális personalization adatok
    personalization_data: Optional[dict] = None
    # Opcionális user lista (ha nincs megadva, akkor az összes user)
    user_emails: Optional[List[str]] = None

@app.get("/notifalse")
async def notifalse():
    """
    Lekéri az összes usert, és mindegyiknél a latestnotivisited mezőt false-ra állítja (PUT-tal, csak Adalo által elvárt mezőkkel és alapértelmezett értékekkel).
    GET kérésre is működik, így elég csak betölteni az URL-t.
    """
    app_id = ADALO_USERS_APP_ID
    collection_id = ADALO_USERS_COLLECTION_ID
    api_key = ADALO_API_KEY

    users_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    allowed_fields = [
        "Email", "valami", "Full Name", "Transactions (jouser_transact)s", "level_name", "liked_coupons",
        "unlocked_coupons", "registration_date", "latesthunicoinlogin", "wantsto_delete", "verified_time",
        "student_verified", "current_card", "transactions_user", "total_hunicoins", "diakigazolvany_azonosito",
        "nickname", "gender", "latestnotivisited", "liked_partners", "hunidate", "disliked_categories",
        "liked_categories", "level_url", "Admin?"
    ]
    default_values = {
        "Email": "",
        "valami": "",
        "Full Name": "",
        "Transactions (jouser_transact)s": [],
        "level_name": "",
        "liked_coupons": [],
        "unlocked_coupons": [],
        "registration_date": "",
        "latesthunicoinlogin": "",
        "wantsto_delete": "",
        "verified_time": "",
        "student_verified": False,
        "current_card": [],
        "transactions_user": [],
        "total_hunicoins": 0,
        "diakigazolvany_azonosito": 0,
        "nickname": "",
        "gender": "",
        "latestnotivisited": False,
        "liked_partners": [],
        "hunidate": "",
        "disliked_categories": [],
        "liked_categories": [],
        "level_url": "",
        "Admin?": False
    }

    response = requests.get(users_url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Adalo API hiba: {response.text}")
    data = response.json()
    users = data.get("records", [])

    updated = 0
    errors = []
    for user in users:
        user_id = user.get("id")
        if user_id is None:
            continue
        get_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}/{user_id}"
        get_resp = requests.get(get_url, headers=headers)
        if get_resp.status_code != 200:
            errors.append({"user_id": user_id, "status": get_resp.status_code, "body": get_resp.text, "step": "get"})
            continue
        full_record = get_resp.json()
        filtered_record = {}
        for k in allowed_fields:
            v = full_record.get(k, None)
            if v is None:
                filtered_record[k] = default_values[k]
            else:
                filtered_record[k] = v
        filtered_record["latestnotivisited"] = False
        put_url = get_url
        put_resp = requests.put(put_url, headers=headers, json=filtered_record)
        if put_resp.status_code in [200, 201]:
            updated += 1
        else:
            errors.append({"user_id": user_id, "status": put_resp.status_code, "body": put_resp.text, "step": "put", "sent": filtered_record})

    return {
        "updated_users": updated,
        "errors": errors,
        "total_users": len(users)
    }

@app.post("/get-partner-transactions")
async def get_partner_transactions(request_data: GetTransactionsRequest):
    """
    Lekéri egy partner összes 'finalized' tranzakcióját és JSON-ként visszaadja (nincs API kulcs védelem)
    Ez a végpont Adalo custom function-ök számára készült.
    """
    partner_id = request_data.partner_id

    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print(f"\n=== Új kérés kezdése partner_id={partner_id} ===")
    
    # Adalo API hívás
    url = f"https://api.adalo.com/v0/apps/{ADALO_TRANSACTIONS_APP_ID}/collections/{ADALO_TRANSACTIONS_COLLECTION_ID}"
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

@app.get("/download-transactions/{partner_id}")
async def download_partner_transactions(
    partner_id: int,
    background_tasks: BackgroundTasks,
    from_date: Optional[str] = Query(None, description="Optional: Szűrés ettől a dátumtól (DD/MM/YYYY formátum)"),
    to_date: Optional[str] = Query(None, description="Optional: Szűrés eddig a dátumig (DD/MM/YYYY formátum)")
):
    """
    Lekéri egy partner összes 'finalized' tranzakcióját és Excel fájlként visszaadja.
    Szűrhető a tranzakció dátuma alapján (updated_at) egy megadott időszakban.
    Ez a végpont közvetlen böngésző vagy Adalo 'Open Website' híváshoz készült.
    """
    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print(f"\n=== Új Excel letöltési kérés kezdése partner_id={partner_id} ===")
    
    # Adalo API hívás
    url = f"https://api.adalo.com/v0/apps/{ADALO_TRANSACTIONS_APP_ID}/collections/{ADALO_TRANSACTIONS_COLLECTION_ID}"
    headers = {
        "Authorization": f"Bearer {ADALO_API_KEY}",
        "Content-Type": "application/json"
    }
    
    print(f"API URL: {url}")
    print(f"API Headers: {headers}")
    
    try:
        print("Adalo API hívás indítása (Excel végpont)...")
        response = requests.get(url, headers=headers)
        print(f"Adalo API válasz státuszkód: {response.status_code}")
        print(f"Adalo API válasz fejlécek: {dict(response.headers)}")
        
        if response.status_code != 200:
            print(f"Hibás Adalo API válasz (Excel végpont): {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Adalo API hiba: {response.text}"
            )
        
        print("Adalo API válasz feldolgozása (Excel végpont)...")
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
                    #print(f"Hiányzó 'records' kulcs az Adalo API válaszból (Excel végpont).")
                    #raise HTTPException(
                    #    status_code=500,
                    #    detail=f"Hiányzó 'records' kulcs az Adalo API válaszból (Excel végpont)."
                    #)
            elif not isinstance(transactions, list):
                raise HTTPException(
                    status_code=500,
                    detail=f"Váratlan Adalo API válasz formátum (Excel végpont): {type(transactions)}"
                )
            
            print(f"Összes Adalo tranzakció száma (Excel végpont): {len(transactions)}")
            
            # Dátum paraméterek feldolgozása
            from_datetime = None
            if from_date:
                try:
                    # Próbáljuk meg DD/MM/YYYY formátumként értelmezni, és a nap elejére (UTC) konvertálni
                    from_datetime = datetime.strptime(from_date, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                    print(f"Szűrési kezdő dátum: {from_datetime}")
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail="Érvénytelen dátum formátum a from_date paraméterben. Használd a DD/MM/YYYY formátumot."
                    )

            to_datetime = None
            if to_date:
                try:
                    # Próbáljuk meg DD/MM/YYYY formátumként értelmezni, és a nap végére (UTC) konvertálni
                    # Hozzáadunk 1 napot és visszamegyünk 1 másodpercet, hogy a nap végét is magába foglalja
                    to_datetime = datetime.strptime(to_date, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                    to_datetime = to_datetime + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                    print(f"Szűrési záró dátum: {to_datetime}")
                except ValueError:
                     raise HTTPException(
                        status_code=400,
                        detail="Érvénytelen dátum formátum a to_date paraméterben. Használd a DD/MM/YYYY formátumot."
                    )

            # Dátum tartomány validálása (ha mindkettő meg van adva)
            if from_datetime and to_datetime and from_datetime > to_datetime:
                 raise HTTPException(
                     status_code=400,
                     detail="A from_date nem lehet későbbi, mint a to_date."
                 )

            
            # Szűrés partner ID, státusz és dátum tartomány alapján
            finalized_partner_transactions = []
            for t in transactions:
                if isinstance(t, dict):
                    if t.get("transaction_status") == "finalized":
                         if "partner_transaction" in t:
                            if partner_id in t["partner_transaction"]:
                                # Dátum szűrés
                                transaction_date_str = t.get("updated_at")
                                if transaction_date_str:
                                    try:
                                        # Adalo dátum string átalakítása datetime objektummá (UTC-ben)
                                        updated_at_datetime = datetime.strptime(transaction_date_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                                        
                                        # Ellenőrzés a dátum tartományra
                                        include_transaction = True
                                        if from_datetime and updated_at_datetime < from_datetime:
                                            include_transaction = False
                                        if to_datetime and updated_at_datetime > to_datetime:
                                            include_transaction = False
                                        
                                        if include_transaction:
                                             finalized_partner_transactions.append(t)

                                    except (ValueError, TypeError) as e:
                                        print(f"Figyelmeztetés: Hibás updated_at formátum a tranzakcióban (id: {t.get('id')}, érték: {transaction_date_str}). Kihagyva a dátum szűrésből: {e}")
                                        # Ha a dátum formátum hibás, kihagyjuk a tranzakciót
                                        pass
                                elif from_datetime or to_datetime:
                                     # Ha van dátumszűrő, de a tranzakción nincs updated_at, akkor kihagyjuk
                                     print(f"Figyelmeztetés: Hiányzó updated_at a tranzakcióban (id: {t.get('id')}). Kihagyva a dátum szűrésből.")
                                     pass
                                else:
                                    # Nincs dátum szűrés, és van partner ID, státusz, és partner_transaction
                                    finalized_partner_transactions.append(t)
            
            print(f"Talált 'finalized' partner tranzakciók száma (Excel végpont, dátum szűrővel): {len(finalized_partner_transactions)}")
            
            if not finalized_partner_transactions:
                 # Excel végponton 404-et adunk vissza, ha nincs adat
                 raise HTTPException(
                    status_code=404,
                    detail=f"Nem található 'finalized' tranzakció a partner_id={partner_id} számára"
                 )
            
            # Kuponok lekérdezése és coupon_name hozzáadása a DataFrame létrehozása ELŐTT
            print("Kuponok lekérdezése a coupon_name mezőhöz...")
            coupons_url = f"https://api.adalo.com/v0/apps/{ADALO_COUPONS_APP_ID}/collections/{ADALO_COUPONS_COLLECTION_ID}"
            coupons_headers = {
                "Authorization": f"Bearer {ADALO_TRANSACTIONS_API_KEY}",
                "Content-Type": "application/json"
            }
            
            coupons_dict = {}
            try:
                coupons_response = requests.get(coupons_url, headers=coupons_headers)
                if coupons_response.status_code == 200:
                    coupons_data = coupons_response.json()
                    coupons = coupons_data.get("records", [])
                    for coupon in coupons:
                        coupons_dict[coupon.get("id")] = coupon.get("coupon_name", "")
                    print(f"Sikeresen betöltött {len(coupons_dict)} kupon")
                else:
                    print(f"Kuponok lekérdezése sikertelen: {coupons_response.status_code}")
            except Exception as e:
                print(f"Hiba a kuponok lekérdezése során: {str(e)}")
            
            # coupon_name hozzáadása a tranzakciókhoz MINDEN tranzakcióhoz
            print("Coupon_name hozzáadása a tranzakciókhoz...")
            for transaction in finalized_partner_transactions:
                coupon_ids = transaction.get("coupon_transaction", [])
                if coupon_ids and isinstance(coupon_ids, list) and len(coupon_ids) > 0:
                    coupon_id = coupon_ids[0]  # Első kupon ID használata
                    transaction["coupon_name"] = coupons_dict.get(coupon_id, "")
                    print(f"Tranzakció {transaction.get('id')}: coupon_id={coupon_id}, coupon_name='{transaction['coupon_name']}'")
                else:
                    transaction["coupon_name"] = ""
                    print(f"Tranzakció {transaction.get('id')}: nincs kupon")
            
            # DataFrame létrehozása a coupon_name hozzáadása UTÁN
            print("DataFrame létrehozása (Excel végpont)...")
            df = pd.DataFrame(finalized_partner_transactions)
            print(f"DataFrame oszlopok (eredeti): {list(df.columns)}")
            
            # Kívánt oszlopok kiválasztása és átnevezése
            desired_columns = [
                "id",
                "transaction_status",
                "user_transaction",
                "partner_transaction", # Eredetileg kért oszlop, de a lekeresnel szurtunk ra, user_transaction es coupon_transaction volt helyette
                "coupon_transaction",
                "coupon_name",
                "spend_value",
                "discount_value",
                "saved_value",
                "hunicoin_value",
                "jutalek_value",
                "updated_at"
            ]
            # Ellenőrizzük, hogy a kívánt oszlopok léteznek-e a DataFrame-ben
            existing_columns = [col for col in desired_columns if col in df.columns]
            df = df[existing_columns]
            print(f"DataFrame oszlopok (kiválasztott): {list(df.columns)}")
            
            # Fejlécek átnevezése
            column_mapping = {
                "id": "Tranzakció azonosítója",
                "transaction_status": "Tranzakció státusza",
                "user_transaction": "User id-ja",
                "partner_transaction": "Partner id-ja", # Hozzáadva az átnevezéshez, ha létezik
                "coupon_transaction": "Kupon id-ja",
                "coupon_name": "Kupon neve",
                "spend_value": "Költés",
                "discount_value": "Kedvezmény %",
                "saved_value": "Spórolás",
                "hunicoin_value": "Hunicoinok száma",
                "jutalek_value": "Jutalék összege",
                "updated_at": "Tranzakció dátuma"
            }
            # Csak a kiválasztott oszlopoknak megfelelő mapping használata
            renamed_columns = {old_name: new_name for old_name, new_name in column_mapping.items() if old_name in df.columns}
            df = df.rename(columns=renamed_columns)
            print(f"DataFrame oszlopok (átnevezett): {list(df.columns)}")
            
            # Dátum formázása a 'Tranzakció dátuma' oszlopban
            if "Tranzakció dátuma" in df.columns:
                try:
                    # Átalakítás datetime objektummá
                    # Az Adalo API válasz ISO formátumú, UTC időzónával
                    df["Tranzakció dátuma"] = pd.to_datetime(df["Tranzakció dátuma"], utc=True)
                    # Formázás a kívánt string formátumra (óra, perc)
                    df["Tranzakció dátuma"] = df["Tranzakció dátuma"].dt.strftime('%Y-%m-%d %H:%M')
                    print("Tranzakció dátuma oszlop formázva.")
                except Exception as e:
                    print(f"Hiba a dátum formázása során: {str(e)}")
                    # Hibakezelés: ha nem sikerül formázni, hagyjuk az eredeti értéket
                    pass # Folytatjuk az eredeti dátum formátummal
            else:
                 print("'Tranzakció dátuma' oszlop nem található a formázáshoz.")

            
            # Excel fájl mentése
            filename = f"transactions_partner_{partner_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            print(f"Excel fájl mentése (Excel végpont): {filename}")
            df.to_excel(filename, index=False)
            
            print("Fájl sikeresen létrehozva (Excel végpont)!")
            
            # FileResponse létrehozása
            response = FileResponse(
                filename,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=filename
            )
            
            # Fájl törlése miután elküldtük a BackgroundTasks segítségével
            background_tasks.add_task(os.remove, filename)
            
            return response
            
        except ValueError as e:
            print(f"JSON feldolgozási hiba (Excel végpont): {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Hibás JSON válasz az Adalo API-tól (Excel végpont): {str(e)}"
            )
        
    except requests.exceptions.RequestException as e:
        print(f"Adalo API hívási hiba (Excel végpont): {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Hiba az Adalo API hívás során (Excel végpont): {str(e)}"
        )
    except Exception as e:
         print(f"Váratlan hiba történt (Excel végpont): {str(e)}")
         raise HTTPException(status_code=500, detail=f"Váratlan szerverhiba (Excel végpont): {str(e)}")

@app.get("/download-users")
async def download_users(
    background_tasks: BackgroundTasks,
    from_date: Optional[str] = Query(None, description="Optional: Szűrés ettől a dátumtól (DD/MM/YYYY formátum)"),
    to_date: Optional[str] = Query(None, description="Optional: Szűrés eddig a dátumig (DD/MM/YYYY formátum)")
):
    """
    Lekéri az összes felhasználót az Adalo API-ból és Excel fájlként visszaadja.
    Szűrhető a felhasználó létrehozásának dátuma alapján egy megadott időszakban.
    """
    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print("\n=== Új felhasználó Excel letöltési kérés kezdése ===")
    
    # Adalo Users API hívás
    url = f"https://api.adalo.com/v0/apps/{ADALO_USERS_APP_ID}/users"
    headers = {
        "Authorization": f"Bearer {ADALO_API_KEY}",
        "Content-Type": "application/json"
    }
    
    print(f"API URL: {url}")
    
    try:
        print("Adalo Users API hívás indítása...")
        response = requests.get(url, headers=headers)
        print(f"Adalo API válasz státuszkód: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Hibás Adalo API válasz: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Adalo API hiba: {response.text}"
            )
        
        try:
            users = response.json()
            
            # Adalo API válasz formátum ellenőrzése
            if isinstance(users, dict) and "users" in users:
                users = users["users"]
            elif not isinstance(users, list):
                raise HTTPException(
                    status_code=500,
                    detail=f"Váratlan Adalo API válasz formátum: {type(users)}"
                )
            
            print(f"Összes felhasználó száma: {len(users)}")
            
            # Dátum paraméterek feldolgozása
            from_datetime = None
            if from_date:
                try:
                    from_datetime = datetime.strptime(from_date, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                    print(f"Szűrési kezdő dátum: {from_datetime}")
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail="Érvénytelen dátum formátum a from_date paraméterben. Használd a DD/MM/YYYY formátumot."
                    )

            to_datetime = None
            if to_date:
                try:
                    to_datetime = datetime.strptime(to_date, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                    to_datetime = to_datetime + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                    print(f"Szűrési záró dátum: {to_datetime}")
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail="Érvénytelen dátum formátum a to_date paraméterben. Használd a DD/MM/YYYY formátumot."
                    )

            # Dátum tartomány validálása
            if from_datetime and to_datetime and from_datetime > to_datetime:
                raise HTTPException(
                    status_code=400,
                    detail="A from_date nem lehet későbbi, mint a to_date."
                )

            # Szűrés dátum alapján
            filtered_users = []
            for user in users:
                if isinstance(user, dict):
                    # Email ellenőrzés - csak nem üres email címmel rendelkező userek
                    email = user.get("email", "")
                    if not email:
                        continue

                    created_at_str = user.get("created_at")
                    if created_at_str:
                        try:
                            created_at = datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)

                            include_user = True
                            if from_datetime and created_at < from_datetime:
                                include_user = False
                            if to_datetime and created_at > to_datetime:
                                include_user = False

                            if include_user:
                                filtered_users.append(user)
                        except (ValueError, TypeError) as e:
                            print(f"Figyelmeztetés: Hibás created_at formátum a felhasználóban (id: {user.get('id')}). Kihagyva: {e}")
                            pass
                    elif from_datetime or to_datetime:
                        print(f"Figyelmeztetés: Hiányzó created_at a felhasználóban (id: {user.get('id')}). Kihagyva.")
                        pass
                    else:
                        filtered_users.append(user)

            print(f"Szűrt felhasználók száma: {len(filtered_users)}")

            if not filtered_users:
                raise HTTPException(
                    status_code=404,
                    detail="Nem található felhasználó a megadott feltételek alapján"
                )

            # DataFrame létrehozása
            df = pd.DataFrame(filtered_users)

            # Kívánt oszlopok kiválasztása és átnevezése
            column_mapping = {
                "id": "Felhasználó azonosító",
                "email": "Email cím",
                "phone": "Telefonszám",
                "created_at": "Regisztráció dátuma",
                "updated_at": "Utolsó módosítás dátuma",
                "first_name": "Keresztnév",
                "last_name": "Vezetéknév",
                "status": "Státusz"
            }
            
            # Csak azokat az oszlopokat választjuk ki, amelyek léteznek a DataFrame-ben
            existing_columns = [col for col in column_mapping.keys() if col in df.columns]
            df = df[existing_columns]
            
            # Oszlopok átnevezése
            df = df.rename(columns={old: new for old, new in column_mapping.items() if old in df.columns})
            
            # Dátum formázás
            for date_col in ["Regisztráció dátuma", "Utolsó módosítás dátuma"]:
                if date_col in df.columns:
                    try:
                        df[date_col] = pd.to_datetime(df[date_col], utc=True)
                        df[date_col] = df[date_col].dt.strftime('%Y-%m-%d %H:%M')
                    except Exception as e:
                        print(f"Hiba a {date_col} formázása során: {str(e)}")
                        pass

            # Excel fájl mentése
            filename = f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            df.to_excel(filename, index=False)
            
            # FileResponse létrehozása
            response = FileResponse(
                filename,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=filename
            )
            
            # Fájl törlése miután elküldtük
            background_tasks.add_task(os.remove, filename)
            
            return response
            
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

@app.get("/download-users-collection")
async def download_users_collection(
    background_tasks: BackgroundTasks,
    from_date: Optional[str] = Query(None, description="Optional: Szűrés ettől a dátumtól (DD/MM/YYYY formátum)"),
    to_date: Optional[str] = Query(None, description="Optional: Szűrés eddig a dátumig (DD/MM/YYYY formátum)")
):
    """
    Lekéri az összes felhasználót az Adalo Users Collection API-ból és Excel fájlként visszaadja.
    Szűrhető a felhasználó létrehozásának dátuma alapján egy megadott időszakban.
    """
    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print("\n=== Új felhasználó Collection Excel letöltési kérés kezdése ===")
    
    # Adalo Users Collection API hívás
    url = f"https://api.adalo.com/v0/apps/{ADALO_USERS_APP_ID}/collections/{ADALO_USERS_COLLECTION_ID}"
    headers = {
        "Authorization": f"Bearer {ADALO_API_KEY}",
        "Content-Type": "application/json"
    }
    
    print(f"API URL: {url}")
    
    try:
        print("Adalo Users Collection API hívás indítása...")
        response = requests.get(url, headers=headers)
        print(f"Adalo API válasz státuszkód: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Hibás Adalo API válasz: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Adalo API hiba: {response.text}"
            )
        
        try:
            data = response.json()
            users = data.get("records", [])
            
            print(f"Összes felhasználó száma: {len(users)}")
            
            # Dátum paraméterek feldolgozása
            from_datetime = None
            if from_date:
                try:
                    from_datetime = datetime.strptime(from_date, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                    print(f"Szűrési kezdő dátum: {from_datetime}")
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail="Érvénytelen dátum formátum a from_date paraméterben. Használd a DD/MM/YYYY formátumot."
                    )

            to_datetime = None
            if to_date:
                try:
                    to_datetime = datetime.strptime(to_date, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                    to_datetime = to_datetime + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                    print(f"Szűrési záró dátum: {to_datetime}")
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail="Érvénytelen dátum formátum a to_date paraméterben. Használd a DD/MM/YYYY formátumot."
                    )

            # Dátum tartomány validálása
            if from_datetime and to_datetime and from_datetime > to_datetime:
                raise HTTPException(
                    status_code=400,
                    detail="A from_date nem lehet későbbi, mint a to_date."
                )

            # Szűrés dátum alapján
            filtered_users = []
            for user in users:
                if isinstance(user, dict):
                    # Email ellenőrzés - csak nem üres email címmel rendelkező userek
                    email = user.get("Email", "")
                    if not email:
                        continue

                    created_at_str = user.get("created_at")
                    if created_at_str:
                        try:
                            created_at = datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)

                            include_user = True
                            if from_datetime and created_at < from_datetime:
                                include_user = False
                            if to_datetime and created_at > to_datetime:
                                include_user = False

                            if include_user:
                                filtered_users.append(user)
                        except (ValueError, TypeError) as e:
                            print(f"Figyelmeztetés: Hibás created_at formátum a felhasználóban (id: {user.get('id')}). Kihagyva: {e}")
                            pass
                    elif from_datetime or to_datetime:
                        print(f"Figyelmeztetés: Hiányzó created_at a felhasználóban (id: {user.get('id')}). Kihagyva.")
                        pass
                    else:
                        filtered_users.append(user)

            print(f"Szűrt felhasználók száma: {len(filtered_users)}")

            if not filtered_users:
                raise HTTPException(
                    status_code=404,
                    detail="Nem található felhasználó a megadott feltételek alapján"
                )

            # DataFrame létrehozása
            df = pd.DataFrame(filtered_users)

            # Kívánt oszlopok kiválasztása és átnevezése
            column_mapping = {
                "id": "Felhasználó azonosító",
                "Email": "Email cím",
                "subscribedtonews": "Hírlevél feliratkozás",
                "Full Name": "Teljes név",
                "nickname": "Becenév",
                "registration_date": "Regisztráció dátuma",
                "student_verified": "Diákigazolvány ellenőrizve",
                "verified_time": "Ellenőrzés dátuma",
                "diakigazolvany_azonosito": "Diákigazolvány azonosító",
                "total_hunicoins": "Hunicoinok száma",
                "gender": "Nem",
                "level_name": "Szint neve",
                "level_url": "Szint URL",
                "hunidate": "Huni dátum",
                "liked_categories": "Kedvelt kategóriák",
                "disliked_categories": "Nem kedvelt kategóriák",
                "liked_partners": "Kedvelt partnerek",
                "transactions_user": "Felhasználó tranzakciói",
                "opened_noticoupon": "Megnyitott értesítési kuponok",
                "Admin?": "Admin",
                "wantsto_delete": "Törölni akar",
                "latestnotivisited": "Utolsó értesítés látogatás"
            }
            
            # Csak azokat az oszlopokat választjuk ki, amelyek léteznek a DataFrame-ben
            existing_columns = [col for col in column_mapping.keys() if col in df.columns]
            df = df[existing_columns]
            
            # Oszlopok átnevezése
            df = df.rename(columns={old: new for old, new in column_mapping.items() if old in df.columns})
            
            # Dátum formázás
            date_columns = ["Regisztráció dátuma", "Ellenőrzés dátuma", "Huni dátum"]
            for date_col in date_columns:
                if date_col in df.columns:
                    try:
                        df[date_col] = pd.to_datetime(df[date_col], utc=True)
                        df[date_col] = df[date_col].dt.strftime('%Y-%m-%d %H:%M')
                    except Exception as e:
                        print(f"Hiba a {date_col} formázása során: {str(e)}")
                        pass

            # Boolean értékek formázása
            boolean_columns = ["Diákigazolvány ellenőrizve", "Admin", "Hírlevél feliratkozás", "Utolsó értesítés látogatás"]
            for bool_col in boolean_columns:
                if bool_col in df.columns:
                    df[bool_col] = df[bool_col].map({True: "Igen", False: "Nem"})

            # Excel fájl mentése
            filename = f"users_collection_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            df.to_excel(filename, index=False)
            
            # FileResponse létrehozása
            response = FileResponse(
                filename,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=filename
            )
            
            # Fájl törlése miután elküldtük
            background_tasks.add_task(os.remove, filename)
            
            return response
            
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

@app.get("/test-users")
async def test_users():
    """
    Lekéri a felhasználók számát és létrehoz egy új rekordot a statisztikák collection-ben
    """
    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print("\n=== Felhasználók számának lekérdezése és statisztika létrehozása ===")
    
    # Adalo Users API hívás
    users_url = f"https://api.adalo.com/v0/apps/{ADALO_USERS_APP_ID}/collections/{ADALO_USERS_COLLECTION_ID}"
    headers = {
        "Authorization": f"Bearer {ADALO_API_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        # Felhasználók lekérdezése
        response = requests.get(users_url, headers=headers)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Adalo API hiba a felhasználók lekérdezésekor: {response.text}"
            )
        
        try:
            data = response.json()
            users = data.get("records", [])
            
            # Mai dátum lekérdezése
            today = datetime.now(timezone.utc)
            
            # Felhasználók számolása, akik a mai napon lettek létrehozva
            users_today = sum(1 for user in users if user.get("created_at") and 
                            datetime.strptime(user["created_at"], '%Y-%m-%dT%H:%M:%S.%fZ').date() == today.date())
            
            # Összes felhasználó száma
            total_users = len(users)
            
            # Új statisztika rekord létrehozása
            stats_url = f"https://api.adalo.com/v0/apps/{ADALO_STATS_APP_ID}/collections/{ADALO_STATS_COLLECTION_ID}"
            
            # Új rekord adatai - módosítva a dátum formátum
            new_record = {
                "user_number": total_users,
                "registered_date": today.strftime("%Y-%m-%dT%H:%M:%S.000Z")  # Pontos formátum az Adalo API-hoz
            }
            
            print(f"\nÚj rekord adatai:")
            print(f"URL: {stats_url}")
            print(f"Headers: {headers}")
            print(f"Body: {new_record}")
            
            # POST kérés az új rekord létrehozásához
            stats_response = requests.post(stats_url, headers=headers, json=new_record)
            
            print(f"\nStatisztika API válasz:")
            print(f"Status code: {stats_response.status_code}")
            print(f"Response headers: {dict(stats_response.headers)}")
            print(f"Response body: {stats_response.text}")
            
            if stats_response.status_code not in [200, 201]:
                error_detail = f"Adalo API hiba a statisztika létrehozásakor: Status {stats_response.status_code}, Response: {stats_response.text}"
                print(f"\nHiba: {error_detail}")
                raise HTTPException(
                    status_code=stats_response.status_code,
                    detail=error_detail
                )
            
            return {
                "date": today.strftime("%Y-%m-%d"),
                "users_today": users_today,
                "total_users": total_users,
                "stats_record_created": True,
                "stats_record": new_record
            }
            
        except ValueError as e:
            raise HTTPException(
                status_code=500,
                detail=f"Hibás JSON válasz az Adalo API-tól: {str(e)}"
            )
        
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=f"Hiba az Adalo API hívás során: {str(e)}"
        )
    except Exception as e:
        print(f"\nVáratlan hiba részletei: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Váratlan szerverhiba: {str(e)}")

@app.get("/deleteuser/{user_id}")
async def deleteuser(user_id: int):
    """
    Lemásolja a felhasználót egy új rekordba, de módosítja az email-t és a full name-t.
    Az eredeti user_id alapján működik.
    """
    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print(f"\n=== Felhasználó másolása user_id={user_id} ===")
    
    # Adalo API konfiguráció
    app_id = ADALO_USERS_APP_ID
    collection_id = ADALO_USERS_COLLECTION_ID
    api_key = ADALO_API_KEY
    
    # URL-ek
    get_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}/{user_id}"
    create_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        # 1. Lekérjük az eredeti felhasználót
        print(f"Eredeti felhasználó lekérdezése: {get_url}")
        get_response = requests.get(get_url, headers=headers)
        
        if get_response.status_code != 200:
            raise HTTPException(
                status_code=get_response.status_code,
                detail=f"Adalo API hiba a felhasználó lekérdezésekor: {get_response.text}"
            )
        
        original_user = get_response.json()
        print(f"Eredeti felhasználó sikeresen lekérdezve")
        
        # 2. Generálunk egyedi azonosítót
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]  # milliszekundumok nélkül
        unique_id = f"delete_user_{timestamp}"
        
        # 3. Létrehozzuk az új rekordot csak az alapvető adatokkal (tömbök nélkül)
        basic_user_data = {
            "Email": f"delete_user_{timestamp}@deleted.com",
            "Full Name": f"Deleted User {timestamp}",
            "valami": original_user.get("valami", ""),
            "registration_date": original_user.get("registration_date", ""),
            "diakigazolvany_azonosito": original_user.get("diakigazolvany_azonosito", 0),
            "student_verified": original_user.get("student_verified", False),
            "verified_time": original_user.get("verified_time", ""),
            "nickname": original_user.get("nickname", ""),
            "latesthunicoinlogin": original_user.get("latesthunicoinlogin", ""),
            "total_hunicoins": original_user.get("total_hunicoins", 0),
            "users_partner": original_user.get("users_partner", None),
            "wantsto_delete": original_user.get("wantsto_delete", None),
            "level_url": original_user.get("level_url", ""),
            "level_name": original_user.get("level_name", ""),
            "hunidate": original_user.get("hunidate", ""),
            "gender": original_user.get("gender", ""),
            "gender_url": original_user.get("gender_url", ""),
            "subscribedtonews": original_user.get("subscribedtonews", False),
            "deleted_date": original_user.get("deleted_date", None),
            "mindentelfogad": original_user.get("mindentelfogad", False),
            "aszf_toggle": original_user.get("aszf_toggle", False),
            "gdpr_toggle": original_user.get("gdpr_toggle", False),
            "szemelyre_toggle": original_user.get("szemelyre_toggle", False),
            "suti_toggle": original_user.get("suti_toggle", False),
            "Admin?": original_user.get("Admin?", False)
        }
        
        print(f"Új felhasználó alapvető adatok létrehozása...")
        print(f"Email: {basic_user_data['Email']}")
        print(f"Full Name: {basic_user_data['Full Name']}")
        
        # 4. POST kérés az új rekord létrehozásához (csak alapvető adatokkal)
        create_response = requests.post(create_url, headers=headers, json=basic_user_data)
        
        print(f"Létrehozási válasz státuszkód: {create_response.status_code}")
        print(f"Létrehozási válasz: {create_response.text}")
        
        if create_response.status_code not in [200, 201]:
            raise HTTPException(
                status_code=create_response.status_code,
                detail=f"Adalo API hiba az új rekord létrehozásakor: {create_response.text}"
            )
        
        created_user = create_response.json()
        new_user_id = created_user.get('id')
        
        print(f"Új felhasználó létrehozva, ID: {new_user_id}")
        
        # 5. PUT kérés az összes mező frissítéséhez (transactions_user nélkül)
        print("Összes mező frissítése PUT kéréssel...")
        
        # Összegyűjtjük az összes mezőt az eredeti user-ből (transactions_user nélkül)
        complete_user_data = {
            "Email": basic_user_data['Email'],
            "valami": basic_user_data['valami'],
            "Full Name": basic_user_data['Full Name'],
            "Transactions (jouser_transact)s": original_user.get("Transactions (jouser_transact)s", []),
            "level_name": basic_user_data['level_name'],
            "liked_coupons": original_user.get("liked_coupons", []),
            "unlocked_coupons": original_user.get("unlocked_coupons", []),
            "registration_date": basic_user_data['registration_date'],
            "mindentelfogad": basic_user_data['mindentelfogad'],
            "latesthunicoinlogin": basic_user_data['latesthunicoinlogin'],
            "wantsto_delete": basic_user_data['wantsto_delete'],
            "deleted_date": basic_user_data['deleted_date'],
            "verified_time": basic_user_data['verified_time'],
            "student_verified": basic_user_data['student_verified'],
            "current_card": original_user.get("current_card", []),
            "total_hunicoins": basic_user_data['total_hunicoins'],
            "gdpr_toggle": basic_user_data['gdpr_toggle'],
            "diakigazolvany_azonosito": basic_user_data['diakigazolvany_azonosito'],
            "nickname": basic_user_data['nickname'],
            "gender": basic_user_data['gender'],
            "subscribedtonews": basic_user_data['subscribedtonews'],
            "liked_partners": original_user.get("liked_partners", []),
            "hunidate": basic_user_data['hunidate'],
            "disliked_categories": original_user.get("disliked_categories", []),
            "szemelyre_toggle": basic_user_data['szemelyre_toggle'],
            "opened_noticoupon": original_user.get("opened_noticoupon", []),
            "aszf_toggle": basic_user_data['aszf_toggle'],
            "liked_categories": original_user.get("liked_categories", []),
            "level_url": basic_user_data['level_url'],
            "gender_url": basic_user_data['gender_url'],
            "suti_toggle": basic_user_data['suti_toggle'],
            "Admin?": basic_user_data['Admin?']
        }
        
        # PUT URL az új user-hez
        put_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}/{new_user_id}"
        
        # PUT kérés az összes mező frissítéséhez
        print(f"PUT kérés küldése: {put_url}")
        put_response = requests.put(put_url, headers=headers, json=complete_user_data)
        
        print(f"PUT válasz státuszkód: {put_response.status_code}")
        
        if put_response.status_code not in [200, 201]:
            print(f"Figyelmeztetés: A tömbök frissítése nem sikerült: {put_response.text}")
        
        # 6. Tranzakciók frissítése - manuális teszt alapján
        print("Tranzakciók frissítése...")
        
        # Lekérjük az eredeti user tranzakcióit
        original_transactions = original_user.get("transactions_user", [])
        print(f"Eredeti user tranzakciói: {len(original_transactions)} db")
        
        updated_transactions = 0
        failed_transactions = 0
        
        if original_transactions:
            for transaction_id in original_transactions:
                # Manuális teszt alapján: transactions app ID és API kulcs
                transaction_url = f"https://api.adalo.com/v0/apps/{ADALO_TRANSACTIONS_APP_ID}/collections/{ADALO_TRANSACTIONS_COLLECTION_ID}/{transaction_id}"
                transaction_headers = {
                    "Authorization": f"Bearer {ADALO_TRANSACTIONS_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                # 1. Lekérjük a tranzakciót
                transaction_response = requests.get(transaction_url, headers=transaction_headers)
                
                if transaction_response.status_code == 200:
                    transaction_data = transaction_response.json()
                    
                    # 2. Frissített tranzakció adatok - teljes payload, csak user_transaction változik
                    updated_transaction_data = {
                        "id": transaction_data.get("id"),
                        "transaction_id": transaction_data.get("transaction_id"),
                        "transaction_status": transaction_data.get("transaction_status"),
                        "user_transaction": [new_user_id],  # Csak az új user ID
                        "partner_transaction": transaction_data.get("partner_transaction", []),
                        "coupon_transaction": transaction_data.get("coupon_transaction"),
                        "spend_value": transaction_data.get("spend_value"),
                        "discount_value": transaction_data.get("discount_value"),
                        "saved_value": transaction_data.get("saved_value"),
                        "hunicoin_value": transaction_data.get("hunicoin_value"),
                        "jutalek_value": transaction_data.get("jutalek_value"),
                        "jouser_transact": transaction_data.get("jouser_transact"),
                        "test_user_transaction": transaction_data.get("test_user_transaction"),
                        "created_at": transaction_data.get("created_at"),
                        "updated_at": transaction_data.get("updated_at")
                    }
                    
                    # Debug: Kiírjuk a payload-ot
                    print(f"   PUT Payload: {updated_transaction_data}")
                    print(f"   PUT Headers: {transaction_headers}")
                    
                    # 3. PUT kérés a tranzakció frissítéséhez
                    transaction_put_response = requests.put(transaction_url, headers=transaction_headers, json=updated_transaction_data)
                    
                    if transaction_put_response.status_code in [200, 201]:
                        updated_transactions += 1
                        print(f"✅ Tranzakció {transaction_id} frissítve")
                    else:
                        failed_transactions += 1
                        print(f"❌ Tranzakció {transaction_id} hiba: {transaction_put_response.status_code}")
                else:
                    failed_transactions += 1
                    print(f"❌ Tranzakció {transaction_id} nem található: {transaction_response.status_code}")
        else:
            print("Nincs tranzakció az eredeti user-ben")
        
        print(f"Frissített: {updated_transactions}, Hibás: {failed_transactions}")
        
        # 7. Eredeti user törlése - csak ha nincs hibás tranzakció (403 hibák kivételével)
        original_user_deleted = False
        print(f"Törlési feltétel ellenőrzése: failed_transactions={failed_transactions}, updated_transactions={updated_transactions}")
        
        # Ha csak 403 hibák vannak (jogosultság probléma), akkor is törölhető
        if failed_transactions == 0 or (failed_transactions > 0 and updated_transactions == 0):
            print("Eredeti user törlése...")
            delete_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}/{user_id}"
            print(f"DELETE URL: {delete_url}")
            delete_response = requests.delete(delete_url, headers=headers)
            print(f"DELETE Status: {delete_response.status_code}")
            
            if delete_response.status_code in [200, 204]:
                original_user_deleted = True
                print(f"✅ Eredeti user {user_id} sikeresen törölve")
            else:
                print(f"❌ Eredeti user {user_id} törlési hiba: {delete_response.status_code}")
                print(f"DELETE Response: {delete_response.text}")
        else:
            print(f"Eredeti user nem törölhető: failed_transactions={failed_transactions}")
        
        return {
            "success": True,
            "message": "Felhasználó sikeresen másolva",
            "original_user_id": user_id,
            "new_user_id": new_user_id,
            "new_email": basic_user_data['Email'],
            "new_full_name": basic_user_data['Full Name'],
            "timestamp": timestamp,
            "arrays_updated": put_response.status_code in [200, 201],
            "transactions_updated": updated_transactions,
            "transactions_failed": failed_transactions,
            "total_transactions": len(original_transactions) if original_transactions else 0,
            "original_user_deleted": original_user_deleted
        }
        
    except requests.exceptions.RequestException as e:
        print(f"Adalo API hívási hiba: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Hiba az Adalo API hívás során: {str(e)}"
        )
    except Exception as e:
        print(f"Váratlan hiba történt: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Váratlan szerverhiba: {str(e)}")

@app.get("/auto-delete-users")
async def auto_delete_users():
    """
    Automatikusan törli azokat a usereket, akiknek a wantsto_delete mezője legalább 30 napja be van állítva.
    Ez a végpont cron job-okhoz készült, naponta egyszer futtatható.
    """
    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print("\n=== Automatikus user törlés kezdése ===")
    
    # Adalo API konfiguráció
    app_id = ADALO_USERS_APP_ID
    collection_id = ADALO_USERS_COLLECTION_ID
    api_key = ADALO_API_KEY
    
    # URL-ek
    users_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        # 1. Lekérjük az összes usert
        print("Összes user lekérdezése...")
        response = requests.get(users_url, headers=headers)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Adalo API hiba a userek lekérdezésekor: {response.text}"
            )
        
        data = response.json()
        users = data.get("records", [])
        print(f"Összesen {len(users)} user található")
        
        # 2. Mai dátum (UTC)
        today = datetime.now(timezone.utc)
        thirty_days_ago = today - pd.Timedelta(days=30)
        
        print(f"Mai dátum: {today}")
        print(f"30 nappal ezelőtt: {thirty_days_ago}")
        
        # 3. Ellenőrizzük minden usert
        users_to_delete = []
        
        for user in users:
            user_id = user.get("id")
            wantsto_delete = user.get("wantsto_delete")
            email = user.get("Email", "")
            
            # Kihagyjuk a már törölt usereket (delete_user-ral kezdődő email)
            if email.startswith("delete_user"):
                print(f"User {user_id} ({email}) kihagyva: már törölt user")
                continue
            
            if wantsto_delete:
                try:
                    # Dátum konvertálása
                    delete_date = datetime.strptime(wantsto_delete, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                    
                    # Ellenőrizzük, hogy legalább 30 napja van-e beállítva
                    if delete_date <= thirty_days_ago:
                        users_to_delete.append({
                            "id": user_id,
                            "email": user.get("Email", "N/A"),
                            "full_name": user.get("Full Name", "N/A"),
                            "wantsto_delete": wantsto_delete,
                            "days_old": (today - delete_date).days
                        })
                        print(f"User {user_id} ({user.get('Email', 'N/A')}) törlendő: {delete_date} ({delete_date.strftime('%Y-%m-%d')})")
                        
                except (ValueError, TypeError) as e:
                    print(f"Figyelmeztetés: Hibás wantsto_delete formátum user {user_id}-nél: {wantsto_delete}")
                    continue
        
        print(f"\nTörlendő userek száma: {len(users_to_delete)}")
        
        # 4. Töröljük a usereket
        deleted_users = []
        failed_deletions = []
        
        for user_info in users_to_delete:
            user_id = user_info["id"]
            print(f"\n--- User {user_id} törlése ---")
            
            try:
                # Használjuk a meglévő /deleteuser logikát
                delete_response = await deleteuser(user_id)
                
                if delete_response.get("success"):
                    deleted_users.append({
                        "id": user_id,
                        "email": user_info["email"],
                        "full_name": user_info["full_name"],
                        "days_old": user_info["days_old"],
                        "new_user_id": delete_response.get("new_user_id"),
                        "new_email": delete_response.get("new_email")
                    })
                    print(f"✅ User {user_id} sikeresen törölve")
                else:
                    failed_deletions.append({
                        "id": user_id,
                        "email": user_info["email"],
                        "error": "deleteuser endpoint hiba"
                    })
                    print(f"❌ User {user_id} törlési hiba")
                    
            except Exception as e:
                failed_deletions.append({
                    "id": user_id,
                    "email": user_info["email"],
                    "error": str(e)
                })
                print(f"❌ User {user_id} kivétel: {str(e)}")
        
        return {
            "success": True,
            "message": "Automatikus törlés befejezve",
            "total_users_checked": len(users),
            "users_to_delete_found": len(users_to_delete),
            "successfully_deleted": len(deleted_users),
            "failed_deletions": len(failed_deletions),
            "deleted_users": deleted_users,
            "failed_users": failed_deletions,
            "execution_date": today.isoformat(),
            "thirty_days_ago": thirty_days_ago.isoformat()
        }
        
    except requests.exceptions.RequestException as e:
        print(f"Adalo API hívási hiba: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Hiba az Adalo API hívás során: {str(e)}"
        )
    except Exception as e:
        print(f"Váratlan hiba történt: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Váratlan szerverhiba: {str(e)}")

@app.post("/sendmails")
async def sendmails(request_data: SendMailsRequest):
    """
    MailerSend bulk email API-val küld emailt az összes usernek.
    Adalo custom action-ból hívható meg JSON paraméterekkel.
    """
    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print(f"\n=== Bulk email küldés kezdése ===")
    print(f"Template ID: {request_data.template_id}")
    print(f"Subject: {request_data.subject}")
    
    # MailerSend API konfiguráció
    MAILERSEND_API_KEY = "mlsn.f16b8868e4730cd3c9f9f5319e2b20c7627b8548541ba811c7a77c9281ce0d2c"
    MAILERSEND_BULK_URL = "https://api.mailersend.com/v1/bulk-email"
    
    mailersend_headers = {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Authorization": f"Bearer {MAILERSEND_API_KEY}"
    }
    
    try:
        # 1. Lekérjük az összes usert az Adalo Collection API-ból (mint a /download-users-collection)
        print("Userek lekérdezése az Adalo Collection API-ból...")
        app_id = ADALO_USERS_APP_ID
        collection_id = ADALO_USERS_COLLECTION_ID
        api_key = ADALO_API_KEY
        
        users_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}"
        adalo_headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(users_url, headers=adalo_headers)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Adalo API hiba a userek lekérdezésekor: {response.text}"
            )
        
        data = response.json()
        users = data.get("records", [])
        print(f"Összesen {len(users)} user található az Adalo Collection API-ból")
        
        # 2. Készítsük el a recipient listát
        valid_users = []
        
        if request_data.user_emails:
            # Ha saját email lista van megadva
            print(f"Saját email lista használata: {len(request_data.user_emails)} email")
            for email in request_data.user_emails:
                if email and not email.startswith("delete_user"):
                    valid_users.append({
                        "email": email,
                        "full_name": "",  # Nincs full name a saját listában
                        "user_id": None
                    })
        else:
            # Ha nincs saját lista, akkor az összes user az Adalo Collection API-ból
            print("Összes user használata az Adalo Collection API-ból")
            for user in users:
                email = user.get("Email", "")
                wantsto_delete = user.get("wantsto_delete")
                
                # Kihagyjuk a törölt usereket és azokat, akik törölni akarnak
                if (email and 
                    not email.startswith("delete_user") and 
                    not wantsto_delete):  # Csak azok, akik NEM akarnak törölni
                    
                    valid_users.append({
                        "email": email,
                        "full_name": user.get("Full Name", ""),
                        "user_id": user.get("id")
                    })
                    print(f"User {user.get('id')} ({email}) hozzáadva")
                else:
                    if email.startswith("delete_user"):
                        print(f"User {user.get('id')} ({email}) kihagyva: már törölt")
                    elif wantsto_delete:
                        print(f"User {user.get('id')} ({email}) kihagyva: törölni akar")
                    else:
                        print(f"User {user.get('id')} ({email}) kihagyva: nincs email")
        
        print(f"Érvényes userek száma: {len(valid_users)}")
        
        if not valid_users:
            return {
                "success": False,
                "message": "Nincs érvényes user email cím",
                "total_users": len(users) if not request_data.user_emails else len(request_data.user_emails),
                "valid_users": 0
            }
        
        # 3. Készítsük el a MailerSend bulk email payload-ot
        # MailerSend bulk endpoint: max 500 email objektum, mindegyik max 50 TO recipient
        # Ha több mint 500 user van, több batch-re kell bontani
        
        batch_size = 500
        total_batches = (len(valid_users) + batch_size - 1) // batch_size
        
        print(f"Batch-ek száma: {total_batches}")
        
        all_bulk_responses = []
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(valid_users))
            batch_users = valid_users[start_idx:end_idx]
            
            print(f"Batch {batch_num + 1}/{total_batches}: {len(batch_users)} user")
            
            # Készítsük el a bulk email objektumokat
            bulk_emails = []
            
            for user in batch_users:
                email_obj = {
                    "from": {
                        "email": request_data.from_email,
                        "name": request_data.from_name
                    },
                    "to": [
                        {
                            "email": user["email"],
                            "name": user["full_name"] if user["full_name"] else None
                        }
                    ],
                    "subject": request_data.subject,
                    "template_id": request_data.template_id
                }
                
                # Personalization hozzáadása, ha van
                if request_data.personalization_data:
                    email_obj["personalization"] = [
                        {
                            "email": user["email"],
                            "data": request_data.personalization_data
                        }
                    ]
                
                bulk_emails.append(email_obj)
            
            # MailerSend bulk API hívás
            print(f"MailerSend bulk API hívás batch {batch_num + 1}...")
            
            bulk_payload = bulk_emails
            
            bulk_response = requests.post(
                MAILERSEND_BULK_URL,
                headers=mailersend_headers,
                json=bulk_payload
            )
            
            print(f"MailerSend válasz státuszkód: {bulk_response.status_code}")
            
            if bulk_response.status_code in [200, 201, 202]:
                bulk_data = bulk_response.json()
                all_bulk_responses.append({
                    "batch_num": batch_num + 1,
                    "status": "success",
                    "bulk_email_id": bulk_data.get("id"),
                    "users_count": len(batch_users)
                })
                print(f"✅ Batch {batch_num + 1} sikeresen elküldve")
            else:
                error_detail = f"MailerSend API hiba batch {batch_num + 1}: {bulk_response.status_code} - {bulk_response.text}"
                print(f"❌ {error_detail}")
                all_bulk_responses.append({
                    "batch_num": batch_num + 1,
                    "status": "error",
                    "error": error_detail,
                    "users_count": len(batch_users)
                })
        
        # 4. Összesítés
        successful_batches = sum(1 for resp in all_bulk_responses if resp["status"] == "success")
        failed_batches = len(all_bulk_responses) - successful_batches
        
        return {
            "success": True,
            "message": "Bulk email küldés befejezve",
            "total_users": len(users),
            "valid_users": len(valid_users),
            "total_batches": total_batches,
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "batch_responses": all_bulk_responses,
            "template_id": request_data.template_id,
            "subject": request_data.subject
        }
        
    except requests.exceptions.RequestException as e:
        print(f"API hívási hiba: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Hiba az API hívás során: {str(e)}"
        )
    except Exception as e:
        print(f"Váratlan hiba történt: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Váratlan szerverhiba: {str(e)}")

@app.get("/ping")
async def ping():
    """
    Egyszerű ping végpont a cron job-okhoz és health check-hez
    """
    return {"status": "ok", "message": "pong", "timestamp": datetime.now(timezone.utc).isoformat()}

if __name__ == "__main__":
    import uvicorn
    # Ezt a blokkot csak lokális fejlesztéshez használjuk.
    # Koyeb/Render más módon indítja el az alkalmazást (pl. gunicorn vagy uvicorn)
    # Ügyelj rá, hogy az ADALO_API_KEY környezeti változó be legyen állítva lokálisan (.env)
    # és a telepítési platformon is.
    uvicorn.run(app, host="0.0.0.0", port=8003) 