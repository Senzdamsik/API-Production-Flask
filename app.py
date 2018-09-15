from flask import Flask, jsonify, request
from flaskext.mysql import MySQL

app = Flask(__name__)
mysql = MySQL()

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = ''
app.config['MYSQL_DATABASE_DB'] = 'SQL baru (Okt 2018)'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'

mysql.init_app(app)

@app.route('/listing')
def get():
		batasx = request.args.get('batas')
		cur = mysql.connect().cursor()
		cur.execute("select * from (select gabungan_nama_data.id_nama_data, gabungan_nama_data.nama_data, " +
		'gabungan_nama_data.instansi, gabungan_nama_data.deskripsi, gabungan_nama_data.sumber, ' +
		'gabungan_nama_data.date_created, gabungan_nama_data.date_modified, ' +
		'gabungan_nama_data.nama_pengunggah, gabungan_nama_data.email, ' +
		'gabungan_nama_data.nama_pengubah, gabungan_nama_data.nama_tag, ' +
		'gabungan_nama_data.kategori, nama_data.nama_kelompok as nama_kelompok_data from ' +
		
		'(select data.id, nama_data.id as id_nama_data, nama_data.nama as nama_data, ' +
		'perusahaan.nama as instansi, nama_data.description as deskripsi,  ' +
		'data.sumber as sumber, data.date_created as date_created, ' +
		'data.date_modified as date_modified, (select username from users ' +  
		'where users.id = data.user_created)  as nama_pengunggah, users.email ' + 
		'as email, (select username from users where users.id = data.user_modified) ' +   
		'as nama_pengubah, tabel2.nama_tag as nama_tag , industri.nama as kategori ' +
		'from data left join nama_data on nama_data.id = data.id_nama_data left join ' +
		'perusahaan on perusahaan.id = data.instansi left join users on users.id ' +
		'= data.user_created left join (select tabel.id, GROUP_CONCAT(tabel.nama_tag ' + 
		'separator '+"','"+') as nama_tag from (select data.id, tagdata.nama as nama_tag ' +
		'from data left join rel_data_tagdata on rel_data_tagdata.id_data = ' +
		'data.id left join tagdata on tagdata.id = rel_data_tagdata.id_tag) ' +
		'as tabel group by tabel.id) as tabel2 on tabel2.id = data.id ' +
		'left join industri on industri.id = data.id_industri group by nama_data.nama) as gabungan_nama_data ' +
		
		'left join nama_data ' +
		'on gabungan_nama_data.id_nama_data = nama_data.id) as gabungan_kelompok_nama_data ' +
		
		'group by nama_kelompok_data order by date_modified desc ' +
		' limit '+str(batasx)+", 5")

		keluaran = cur.fetchall()

		



		
		hasil_json = jsonify({'Mentah' : keluaran})
		return hasil_json

if __name__ == '__main__':
	app.run(host= '0.0.0.0',debug = True)



















# if __name__ == '__main__':
#     app.run()


# @app.route("/lala")
# def home():
#     return 'Hello, Flask!'

#http://127.0.0.1:5000/listing?batas=0
