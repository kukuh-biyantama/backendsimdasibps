import requests
import psycopg2


def connDB():
    conn = psycopg2.connect(
                    host="localhost",
                    port="5432",
                    user="postgres",
                    password="password",
                    database="produksi"
                )
    cursor = conn.cursor()
    return cursor,conn

# Function to retrieve data from API based on kode provinsi
def get_api_data(kode_provinsi):
    # API URL Template
    url = "https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/23/wilayah/{}/key/7342305c0d7d5f9fe5a2f0e4c7d08e57/"
    api_base_url = url.format(kode_provinsi)
    response = requests.get(api_base_url)
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
                cursor,conn = connDB()
                # Menentukan skema yang digunakan
                schema_name = "statistik_bps_simdasi"

                # Membuat tabel jika belum ada dalam skema
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {schema_name}.list_SIMDASI_based_on_area (
                        induk INTEGER,
                        wilayah TEXT,
                        id_tabel TEXT,
                        judul TEXT,
                        judul_en TEXT,
                        kode_tabel TEXT,
                        ketersediaan_tahun TEXT
                    )
                ''')

                # Menyimpan data ke dalam tabel dalam skema
                for entry in selected_data:
                    tabel_values = entry.get('ketersediaan_tahun', [])  # Mengambil nilai dari array 'tabel'
                    
                    for tahun in tabel_values:
                        cursor.execute(f'''
                            INSERT INTO {schema_name}.list_SIMDASI_based_on_area (
                                induk,
                                wilayah,
                                id_tabel,
                                judul,
                                judul_en,
                                kode_tabel,
                                ketersediaan_tahun
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            api_data['data'][1]['induk'],
                            api_data['data'][1]['wilayah'],
                            entry.get('id_tabel', None),
                            entry.get('judul', None),
                            entry.get('judul_en', None),
                            entry.get('kode_tabel', None),
                            tahun
                        ))

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


# Function to fetch all provinsi from the database and process data
def process_all_provinsi():
    try:
        # Establish a connection to the PostgreSQL database
        cursor,conn = connDB()
        # Select all provinsi from the database
        cursor.execute('SELECT kode FROM "statistik_bps_simdasi"."province_SIMDASI"')
        kode_provinsi_list = cursor.fetchall()

        for kode_provinsi in kode_provinsi_list:
            # Extract the actual value from the tuple
            kode_provinsi_value = kode_provinsi[0]

            # Fetch data from the API for the current kode provinsi
            api_data = get_api_data(kode_provinsi_value)

            if api_data:
                # Process the API data as needed
                print(f"Data for kode_provinsi {kode_provinsi_value}:")
                print(api_data)
                print("\n")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        if conn:
            conn.close()

if __name__ == "__main__":
    # Ganti URL API dengan URL yang sesuai
   process_all_provinsi()
