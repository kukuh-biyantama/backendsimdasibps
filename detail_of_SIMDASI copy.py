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

# Fungsi untuk menyimpan data ke database
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

            for data_item in data['data'][1]['data']:
                try:
                    for key, variable in data_item["variables"].items():
                        value = variable.get('value')
                        value_raw = variable.get('value_raw')
                        value_code = variable.get('value_code')

                        desired_data = {
                            "judul_tabel": data['data'][1]['judul_tabel'],
                            "judul_tabel_en": data['data'][1]["judul_tabel_en"],
                            "lingkup": data['data'][1]["lingkup"],
                            "lingkup_id": data['data'][1]["lingkup_id"],
                            "lingkup_en": data['data'][1]["lingkup_en"],
                            "tahun_data": data['data'][1]["tahun_data"],
                            "wilayah": data['data'][1]["wilayah"],
                            "penanggung_jawab": data['data'][1]["penanggung_jawab"],
                            "show_satuan": data['data'][1]["show_satuan"],
                            "id_subject": data['data'][1]["id_subject"],
                            "bab": data['data'][1]["bab"],
                            "bab_en": data['data'][1]["bab_en"],
                            "subject": data['data'][1]["subject"],
                            "subject_en": data['data'][1]["subject_en"],
                            "mms_id": data['data'][1]["mms_id"],
                            "mms_subject": data['data'][1]["mms_subject"],
                            "keterangan_data": json.dumps(data['data'][1]["keterangan_data"]),
                            "label": data_item.get("label"),
                            "label_raw": data_item.get("label_raw"),
                            "satuan": data_item.get("satuan"),
                            "kode_wilayah": data_item.get("kode_wilayah"),
                            "variable_key": key,
                            "variable_value": value,
                            "variable_value_raw": value_raw,
                            "variable_value_code": value_code
                        }

                        result_list.append(desired_data)
                except (KeyError, AttributeError) as e:
                    # Skip this data_item if 'variables' is not found or is not a dictionary
                    print(f"Skipping data_item: {data_item}. Error: {e}")
                    continue

            try:
                cursor, conn = connDB()

                # Tentukan schema dan nama tabel
                schema = "statistik_bps_simdasi"
                table_name = "table_detail_SIMDASI"

                # Buat tabel jika belum ada
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                        id SERIAL PRIMARY KEY,
                        judul_tabel VARCHAR(255),
                        judul_tabel_en VARCHAR(255),
                        lingkup VARCHAR(255),
                        lingkup_id VARCHAR(255),
                        lingkup_en VARCHAR(255),
                        tahun_data INTEGER,
                        wilayah VARCHAR(255),
                        penanggung_jawab VARCHAR(255),
                        show_satuan BOOLEAN,
                        id_subject VARCHAR(255),
                        bab VARCHAR(255),
                        bab_en VARCHAR(255),
                        subject VARCHAR(255),
                        subject_en VARCHAR(255),
                        mms_id INTEGER,
                        mms_subject VARCHAR(255),
                        keterangan_data JSONB,
                        label VARCHAR(255),
                        label_raw VARCHAR(255),
                        satuan VARCHAR(255),
                        kode_wilayah VARCHAR(255),
                        variable_key VARCHAR(255),
                        variable_value VARCHAR(255),
                        variable_value_raw VARCHAR(255),
                        variable_value_code VARCHAR(255)
                    );
                """
                cursor.execute(create_table_query)

                # Eksekusi pernyataan SQL untuk menyimpan data ke dalam tabel
                insert_query = f"""
                    INSERT INTO {schema}.{table_name} (
                        judul_tabel, judul_tabel_en, lingkup, lingkup_id, lingkup_en,
                        tahun_data, wilayah, penanggung_jawab, show_satuan, id_subject,
                        bab, bab_en, subject, subject_en, mms_id, mms_subject, keterangan_data,
                        label, label_raw, satuan, kode_wilayah,
                        variable_key, variable_value, variable_value_raw, variable_value_code
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (judul_tabel, variable_key, ) DO NOTHING;
                """
                # Ubah data keterangan_data menjadi format yang sesuai untuk PostgreSQL
                for row in result_list:
                    cursor.execute(insert_query, (
                        row["judul_tabel"],
                        row["judul_tabel_en"],
                        row["lingkup"],
                        row["lingkup_id"],
                        row["lingkup_en"],
                        row["tahun_data"],
                        row["wilayah"],
                        row["penanggung_jawab"],
                        row["show_satuan"],
                        row["id_subject"],
                        row["bab"],
                        row["bab_en"],
                        row["subject"],
                        row["subject_en"],
                        row["mms_id"],
                        row["mms_subject"],
                        row["keterangan_data"],
                        row["label"],
                        row["label_raw"],
                        row["satuan"],
                        row["kode_wilayah"],
                        row["variable_key"],
                        row["variable_value"],
                        row["variable_value_raw"],
                        row["variable_value_code"]
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
    #save_data_to_database(1500000, 2022, 'WUptSkZUMDlSbXNYSGJJaHpLaHVrQT09')
