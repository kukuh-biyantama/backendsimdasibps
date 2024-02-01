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
def get_api_data(id_subject):
    # API URL Template
    url = "https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/34/id_tabel/{}/key/7342305c0d7d5f9fe5a2f0e4c7d08e57/"
    api_base_url = url.format(id_subject)
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
                    CREATE TABLE IF NOT EXISTS {schema_name}.mastertable_detail_SIMDASI (
                        id_tabel TEXT,
                        judul TEXT,
                        judul_en TEXT,
                        kode_tabel_nasional TEXT,
                        kode_tabel_provinsi TEXT,
                        kode_tabel_kabkota TEXT,
                        kl_penanggungjawab TEXT,
                        level_penyajian TEXT,
                        id_subject TEXT,
                        bab TEXT,
                        bab_en TEXT,
                        subject TEXT,
                        subject_en TEXT,
                        mms_id INTEGER,
                        mms_subject TEXT
                    )
                ''')

                # Menyimpan data ke dalam tabel dalam skema
                for entry in selected_data:  # Mengambil nilai dari array 'tabel'
                    cursor.execute(f'''
                            INSERT INTO {schema_name}.mastertable_detail_SIMDASI (
                            id_tabel,
                            judul,
                            judul_en,
                            kode_tabel_nasional,
                            kode_tabel_provinsi,
                            kode_tabel_kabkota,
                            kl_penanggungjawab,
                            level_penyajian,
                            id_subject,
                            bab,
                            bab_en,
                            subject,
                            subject_en,
                            mms_id,
                            mms_subject
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            entry.get('id_tabel', None),
                            entry.get('judul', None),
                            entry.get('judul_en', None),
                            entry.get('kode_tabel_nasional', None),
                            entry.get('kode_tabel_provinsi', None),
                            entry.get('kode_tabel_kabkota', None),
                            entry.get('kl_penanggungjawab', None),
                            entry.get('level_penyajian', None),
                            entry.get('id_subject', None),
                            entry.get('bab', None),
                            entry.get('bab_en', None),
                            entry.get('subject', None),
                            entry.get('subject_en', None),
                            int(entry.get('mms_id', 0)),
                            entry.get('mms_subject', None),
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
def process_all_mastertable():
    try:
        # Establish a connection to the PostgreSQL database
        cursor,conn = connDB()
        # Select all provinsi from the database
        cursor.execute('SELECT id_subject FROM "statistik_bps_simdasi"."master_table_SIMDASI"')
        idsubject_mastertable_list = cursor.fetchall()

        for id_subject in idsubject_mastertable_list:
            # Extract the actual value from the tuple
            idsubject_mastertable_value = id_subject[0]

            # Fetch data from the API for the current kode provinsi
            api_data = get_api_data(idsubject_mastertable_value)

            if api_data:
                # Process the API data as needed
                print(f"Data for kode_provinsi {idsubject_mastertable_value}:")
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
   process_all_mastertable()
