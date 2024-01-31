import requests
import psycopg2
import json

# Konfigurasi database
def connDB():
    conn = psycopg2.connect(
                    host="10.2.130.45",
                    port="5432",
                    user="tim2",
                    password="suropati02",
                    database="produksi"
                )
    cursor = conn.cursor()
    return cursor, conn

def save_data_to_database(induk, ketersediaan_tahun, id_table):
    url = "https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/25/wilayah/{}/tahun/{}/id_tabel/{}/key/7342305c0d7d5f9fe5a2f0e4c7d08e57/"
    api_url = url.format(induk, ketersediaan_tahun, id_table)
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()
        status = data.get('status')
        data_availability = data.get('data-availability')
        
        if status == 'OK' and data_availability == 'available':
            result_list = []  # List to store records
            if 'data' in data and data['data']:
                # Memastikan 'kolom' ada dalam setiap data
                for data_entry in data['data']:
                    try:
                        if 'kolom' in data_entry:
                            kolom_data = data_entry['kolom']
                            
                            # Menampilkan informasi untuk setiap key
                            for key, value in kolom_data.items():
                                desired_data = {
                                    "variabel_key": key ,
                                    "nama_variabel": value.get('nama_variabel', ''),
                                    "nama_variabel_en": value.get('nama_variabel_en', ''),
                                    "tipe": value.get('tipe', ''),
                                    "angka_desimal_dibelakang_koma": value.get('angka_desimal_dibelakang_koma', ''),
                                    "metadata_indikator": value.get('metadata_indikator', ''),
                                    "metadata_konsep_definisi": value.get('metadata_konsep_definisi', ''),
                                    "metadata_kegunaan": value.get('metadata_kegunaan', ''),
                                    "metadata_keterangan_tambahan": value.get('metadata_keterangan_tambahan', ''),
                                    "metadata_interpretasi": value.get('metadata_interpretasi', ''),
                                    "metadata_rumusan": value.get('metadata_rumusan', []),
                                    "metadata_rumus_html": value.get('metadata_rumus_html', ''),
                                    "metadata_dihasilkan_oleh": value.get('metadata_dihasilkan_oleh', ''),
                                    "tanggal_cut_off": value.get('tanggal_cut_off', '')
                                }
                                result_list.append(desired_data)
                    except (KeyError, AttributeError) as e:
                        print(f"Error processing data: {e}")
            try:
                cursor, conn = connDB()
                schema = "statistik_bps_simdasi"
                table_name = "rumus_kolom_SIMDASI"
                
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                        variabel_key VARCHAR(1000) PRIMARY KEY,
                        nama_variabel TEXT,
                        nama_variabel_en TEXT,
                        tipe TEXT,
                        angka_desimal_dibelakang_koma VARCHAR(1000),
                        metadata_indikator TEXT,
                        metadata_konsep_definisi TEXT,
                        metadata_kegunaan TEXT,
                        metadata_keterangan_tambahan TEXT,
                        metadata_interpretasi TEXT,
                        metadata_rumusan TEXT[],
                        metadata_rumus_html TEXT,
                        metadata_dihasilkan_oleh TEXT,
                        tanggal_cut_off DATE
                    );
                """
                cursor.execute(create_table_query)
                
                # Eksekusi pernyataan SQL untuk menyimpan data ke dalam tabel
                insert_query = f"""
                    INSERT INTO {schema}.{table_name} (
                        variabel_key, nama_variabel, nama_variabel_en, tipe, angka_desimal_dibelakang_koma,
                        metadata_indikator, metadata_konsep_definisi, metadata_kegunaan, metadata_keterangan_tambahan, metadata_interpretasi,
                        metadata_rumusan, metadata_rumus_html, metadata_dihasilkan_oleh, tanggal_cut_off
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (variabel_key) DO NOTHING;
                """
                # Ubah data keterangan_data menjadi format yang sesuai untuk PostgreSQL
                for row in result_list:
                    cursor.execute(insert_query, (
                        row["variabel_key"],
                        row["nama_variabel"],
                        row["nama_variabel_en"],
                        row["tipe"],
                        row["angka_desimal_dibelakang_koma"],
                        row["metadata_indikator"],
                        row["metadata_konsep_definisi"],
                        row["metadata_kegunaan"],
                        row["metadata_keterangan_tambahan"],
                        row["metadata_interpretasi"],
                        row["metadata_rumusan"],
                        row["metadata_rumus_html"],
                        row["metadata_dihasilkan_oleh"],
                        row["tanggal_cut_off"],
                    ))

                # Commit perubahan ke database
                conn.commit()

                print(f"Data berhasil disimpan ke database untuk {induk}, {ketersediaan_tahun}, {id_table}")
                
            except Exception as e:
                print(f"Error saat menyimpan data ke database: {e}")

            finally:
                # Tutup koneksi ke database
                if conn:
                    conn.close()

                
                    
    

def process_all_basearea():
    try:
        # Establish a connection to the PostgreSQL database
        cursor, conn = connDB()
        
        # Select all base areas from the database
        cursor.execute('SELECT induk, ketersediaan_tahun, id_tabel FROM "statistik_bps_simdasi"."list_based_on_area_SIMDASI"')
        response_basearea = cursor.fetchall()

        result_list = []  # List to store results from save_data_to_database()

        for base in response_basearea:
            # Extract the actual values from the tuple
            induk, ketersediaan_tahun, id_table = base

            # Fetch data from the API for the current base area
            api_data = save_data_to_database(induk, ketersediaan_tahun, id_table)

            if api_data:
                # Process the API data as needed
                print(f"Data for base area {base}: {api_data}")

                # Add the result to the list
                result_list.append(api_data)

        return result_list
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        if conn:
            conn.close()

if __name__ == "__main__":
    # Jalankan proses untuk semua base area
    process_all_basearea()

