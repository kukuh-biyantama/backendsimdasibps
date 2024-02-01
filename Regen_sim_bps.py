import requests
import psycopg2


def connDB():
    conn = psycopg2.connect(
                    host="10.2.130.45",
                    port="5432",
                    user="tim2",
                    password="suropati02",
                    database="produksi"
                )
    cursor = conn.cursor()
    return cursor,conn

# Function to retrieve data from API based on kode provinsi
def get_api_data(kode_provinsi):
    # API URL Template
    url = "https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/27/parent/{}/key/7342305c0d7d5f9fe5a2f0e4c7d08e57/"
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
                    CREATE TABLE IF NOT EXISTS {schema_name}.regency_SIMDASI (
                        lingkup TEXT,
                        versi TEXT,
                        induk INTEGER,
                        wilayah TEXT,
                        kode INTEGER,
                        nama TEXT,
                        satuan_lingkungan TEXT,
                        satuan_lingkungan_en TEXT
                    )
                ''')

                # Menyimpan data ke dalam tabel dalam skema
                for entry in selected_data:
                    cursor.execute(f'''
                        INSERT INTO {schema_name}.regency_SIMDASI (lingkup, versi, induk, wilayah, kode, nama, satuan_lingkungan, satuan_lingkungan_en)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        api_data['data'][1]['lingkup'],
                        api_data['data'][1]['versi'],
                        api_data['data'][1]['induk'],
                        api_data['data'][1]['wilayah'],
                        int(entry.get('kode', 0)),
                        entry.get('nama', None),
                        entry.get('satuan_lingkungan', None),
                        entry.get('satuan_lingkungan_en', None)
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
        cursor.execute('SELECT kode FROM "statistik_bps_simdasi"."SIMDASI_Province"')
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
