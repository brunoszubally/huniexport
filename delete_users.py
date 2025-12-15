import requests

# User ID-k listája
user_ids = [
   263, 262, 261,260,259, 258, 257, 256, 255, 254, 253,252
]

# Adalo API konfiguráció
app_id = "78abf0f7-0d48-492e-98b5-ee301ebe700e"
collection_id = "t_0013b9f2134b4b79b0820993b01145d4"
api_key = "2oq7qmxcjwa4m1tcqdf1w1e8i"

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
}

print(f"Törlendő userek száma: {len(user_ids)}")
print("=" * 50)

deleted_count = 0
failed_count = 0

for user_id in user_ids:
    delete_url = f"https://api.adalo.com/v0/apps/{app_id}/collections/{collection_id}/{user_id}"
    
    try:
        response = requests.delete(delete_url, headers=headers)
        
        if response.status_code in [200, 204]:
            print(f"✅ User {user_id} sikeresen törölve")
            deleted_count += 1
        else:
            print(f"❌ User {user_id} törlési hiba: {response.status_code}")
            print(f"   Response: {response.text}")
            failed_count += 1
            
    except Exception as e:
        print(f"❌ User {user_id} hiba: {str(e)}")
        failed_count += 1

print("=" * 50)
print(f"Összesítés:")
print(f"Sikeresen törölve: {deleted_count}")
print(f"Hibás: {failed_count}")
print(f"Összesen: {len(user_ids)}") 