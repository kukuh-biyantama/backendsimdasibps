import requests
import psycopg2
def scrape_api_data(api_url):
    response = requests.get(api_url)
    # Periksa apakah permintaan berhasil (status code 200)
    if response.status_code == 200:
        api_data = response.json()
        status = api_data.get('status')
        data_availability = api_data.get('data-availability')

        # Periksa apakah data tersedia
        if status == 'OK' and data_availability == 'available':
            # Ambil data yang diinginkan
            selected_data = api_data['data'][1]['data']  # Sesuaikan indeks sesuai dengan struktur respons

            # Step 4: Menyimpan Data ke Database (PostgreSQL)
            try:
                conn = psycopg2.connect(
                    host="10.2.130.45",
                    port="5432",
                    user="tim2",
                    password="suropati02",
                    database="produksi"
                )
                cursor = conn.cursor()

                # Menentukan skema yang digunakan
                schema_name = "statistik_bps_simdasi"

                # Membuat tabel jika belum ada dalam skema
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {schema_name}.simdasi_provinsi (
                        kode INTEGER,
                        nama TEXT,
                        satuan_lingkungan TEXT,
                        satuan_lingkungan_en TEXT
                    )
                ''')

                # Menyimpan data ke dalam tabel dalam skema
                for entry in selected_data:
                    cursor.execute(f'''
                        INSERT INTO {schema_name}.simdasi_provinsi (kode, nama, satuan_lingkungan, satuan_lingkungan_en)
                        VALUES (%s, %s, %s, %s)
                    ''', (int(entry['kode']), entry['nama'], entry['satuan_lingkungan'], entry['satuan_lingkungan_en']))

                # Commit perubahan dan menutup koneksi
                conn.commit()
                conn.close()

                print("Data berhasil diambil dari API dan disimpan ke dalam database.")
            except Exception as e:
                print(f"Error: {e}")
        else:
            print("Data tidak tersedia atau terdapat kesalahan dalam respons API.")
    else:
        print(f"Failed to fetch data from API. Status code: {response.status_code}")

if __name__ == "__main__":
    # Ganti URL API dengan URL yang sesuai
    api_url = "https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/26/key/7342305c0d7d5f9fe5a2f0e4c7d08e57/"
    scrape_api_data(api_url)
