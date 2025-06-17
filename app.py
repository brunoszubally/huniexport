from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
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

app = FastAPI(title="Huniexport API")

# Adalo API konfiguráció
ADALO_APP_ID = "105b8ea3-f2e9-498e-b939-03d445237d78"
ADALO_COLLECTION_ID = "t_e11t5tqgg6jbkbq4a1z596kqt"

# API kulcs beállítása: először környezeti változóból, ha nincs, akkor közvetlenül a kódban
ADALO_API_KEY = os.getenv("ADALO_API_KEY","2f7hg3qfd2fctfrf3argfal9d")

# Saját API kulcs az autentikációhoz (eltávolítva)
# API_KEY = os.getenv("SERVICE_API_KEY")
# api_key_header = APIKeyHeader(name="X-API-Key")

# def get_api_key(api_key: str = Depends(api_key_header)):
#     if api_key != API_KEY:
#         raise HTTPException(status_code=401, detail="Érvénytelen API kulcs")
#     return api_key

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
    request_data: GetTransactionsRequest
    # api_key: str = Depends(get_api_key) # Autentikáció eltávolítva
):
    """
    Lekéri egy partner összes 'finalized' tranzakcióját és JSON-ként visszaadja (nincs API kulcs védelem)
    Ez a végpont Adalo custom function-ök számára készült.
    """
    partner_id = request_data.partner_id

    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

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

# Új végpont az Excel letöltéshez (közvetlen híváshoz Adaloból vagy böngészőből)
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
    url = f"https://api.adalo.com/v0/apps/{ADALO_APP_ID}/collections/{ADALO_COLLECTION_ID}"
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
            
            # DataFrame létrehozása
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

# Új végpont a felhasználók Excel letöltéséhez
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
    url = f"https://api.adalo.com/v0/apps/{ADALO_APP_ID}/users"
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

# Teszt végpont az Adalo API válasz megjelenítéséhez
@app.get("/test-users")
async def test_users():
    """
    Lekéri a felhasználók számát és létrehoz egy új rekordot a statisztikák collection-ben
    """
    if not ADALO_API_KEY:
        raise HTTPException(status_code=500, detail="Adalo API kulcs nincs beállítva (ADALO_API_KEY környezeti változó)")

    print("\n=== Felhasználók számának lekérdezése és statisztika létrehozása ===")
    
    # Adalo Users API hívás
    users_url = f"https://api.adalo.com/v0/apps/{ADALO_APP_ID}/collections/t_0013b9f2134b4b79b0820993b01145d4"
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
            stats_url = f"https://api.adalo.com/v0/apps/{ADALO_APP_ID}/collections/t_ashzitr0lvm0u1dibo7jada75"
            
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

# Egyszerű ping végpont a cron job-okhoz
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
    uvicorn.run(app, host="0.0.0.0", port=8000) 