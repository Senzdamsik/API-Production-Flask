from flask import Flask, jsonify, request
from flaskext.mysql import MySQL
import pandas as pd
import numpy as np

app = Flask(__name__)
mysql = MySQL()

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = ''
app.config['MYSQL_DATABASE_DB'] = 'new_schema'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)

url = "http://127.0.0.1:5000/download?data="

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

	link = []
	for a in range(len(keluaran)):
		link.append(url+"+".join(keluaran[a][12].split(" ")))

	

	No = []
	for b in range(20):
		No.append(b+1)

	date_createdx = []
	for c in range(len(keluaran)):
		date_createdx.append(keluaran[c][5])

	date_modifiedx = []
	for d in range(len(keluaran)):
		date_modifiedx.append(keluaran[d][6])

	nama_pengunggahx = []
	for e in range(len(keluaran)):
		nama_pengunggahx.append(keluaran[e][7])

	nama_kelompok_datax = []
	for f in range(len(keluaran)):
		nama_kelompok_datax.append(keluaran[f][12])

	nama_instansix = []
	for g in range(len(keluaran)):
		nama_instansix.append(keluaran[g][2])

	desx = []
	for h in range(len(keluaran)):
		desx.append(keluaran[h][3])

	sumberx = []
	for i in range(len(keluaran)):
		sumberx_split = keluaran[i][4].split(",")
		sumberx.append(sumberx_split[i])
	
	emailx = []
	for j in range(len(keluaran)):
		emailx.append(keluaran[j][8])

	nama_pengubahx = []
	for l in range(len(keluaran)):
		nama_pengubahx.append(keluaran[l][9])

	tag = []
	for m in range(len(keluaran)):
		tag.append(keluaran[m][10])

	kategori = []
	for n in range(len(keluaran)):
		kategori.append(keluaran[n][11])


	desx2 = []
	for k in range(len(desx)):
		try:
			ubah = desx[k]
			ubah2 = ubah.replace("<p>", "")
			desx2 = ubah2.replace("</p>", "")
		except IndexError:
			pass

	

	output = {}
	outputx = []
	for q in range(20):
		output[No[q]] = {}
		try:
			output[No[q]]["Link"] = link[q]
		except IndexError:
			output[No[q]]["Link"] = str("-")
		try:
			output[No[q]]["Nama Kelompok Data"] = nama_kelompok_datax[q]
		except IndexError:
			output[No[q]]["Nama Kelompok Data"] = str("-")
		try:
			output[No[q]]["Instansi"] = nama_instansix[q]
		except IndexError:
			output[No[q]]["Instansi"] = str("-")
		try:
			output[No[q]]["Date Created"] = date_createdx[q]
		except IndexError:
			output[No[q]]["Date Created"] = str("-")
		try:
			output[No[q]]["Date Modified"] = date_modifiedx[q]
		except IndexError:
			output[No[q]]["Date Modified"] = str("-")
		try:
			output[No[q]]["Nama Pengunggah"] = nama_pengunggahx[q]
		except IndexError:
			output[No[q]]["Nama Pengunggah"] = str("-")
		try:
			output[No[q]]["Email Pengunggah"] = emailx[q]
		except IndexError:
			output[No[q]]["Email Pengunggah"] = str("-")
		try:
			output[No[q]]["Nama Pengubah"] = nama_pengubahx[q]
		except IndexError:
			output[No[q]]["Nama Pengubah"] = str("-")
		try:
			output[No[q]]["Sumber"] = sumberx[q]
		except IndexError:
			output[No[q]]["Sumber"] = str("-")
		try:
			output[No[q]]["Kategori"] = kategori[q]
		except IndexError:
			output[No[q]]["Kategori"] = str("-")
		try:
			output[No[q]]["Deskripsi"] = desx2[q]
		except IndexError:
			output[No[q]]["Deskripsi"] = str("-")	

		try:
			tagx = tag[q].split(",")
			output[No[q]]["Nama Tag"] = {}
			for r in range(len(tagx)):
				output[No[q]]["Nama Tag"][str(r+1)] = tagx[r]
		except:
			pass
	

	lanjut = 'http://127.0.0.1:5000/listing?batas='+str(int(batasx)+int(20))
	hasil_json = jsonify({'Mentah' : output}, {'Lanjut' : lanjut})
	
	return hasil_json


###################################################################################################################################	
###################################################################################################################################	
###################################################################################################################################	
###################################################################################################################################	
###################################################################################################################################


@app.route('/download')
def download():
	datax = request.args.get('data')
	rowx = request.args.get('row')


	data3 = " ".join(datax.split("+"))
	data4 = str("'"+data3+"'")

	cur = mysql.connect().cursor()
	cur.execute('select id, Nama_Data, Waktu, Nilai, Nama_Produk, Item, Negara, Provinsi, ' +
    'Kota, Satuan, Sumber from (select a.id as id, b.nama as Nama_Data, a.data_x as Waktu, a.data_y as Nilai, ' +
    'c.nama as Nama_Produk, d.nama as Item, e.nama as Negara, ' +
    'f.nama as Provinsi, g.nama as Kota, a.satuan as Satuan, ' +
    'a.sumber as Sumber from data a left join nama_data b ' +
    'ON a.id_nama_data = b.id left join produk c ON a.id_produk=c.id left join item d ' +
    'ON a.id_item = d.id left join negara e ON a.id_negara = e.id left join provinsi f ' +
    'ON a.id_provinsi = f.id left join kota g ON a.id_kota = g.id WHERE b.nama_kelompok = '+data4+') ' + 
    'as seluruh ')

	keluaran = cur.fetchall()

	tampung_sementara = []
	for aa in range(len(keluaran)):
		
		data_x1 = keluaran[aa][2].split(",")
		tampung_x = []
		for i in range(len(data_x1)):
			tampung_x.append(data_x1[i])

		data_y1 = keluaran[aa][3].split(",")
		tampung_y = []
		for j in range(len(data_y1)):
			tampung_y.append(data_y1[j])

		tampung_nama_data = []
		for l in range(len(data_x1)):
			tampung_nama_data.append(keluaran[aa][1])

		tampung_nama_produk = []
		for m in range(len(data_x1)):
			tampung_nama_produk.append(keluaran[aa][4])

		tampung_item = []
		for n in range(len(data_x1)):
			tampung_item.append(keluaran[aa][5])

		tampung_negara = []
		for p in range(len(data_x1)):
			tampung_negara.append(keluaran[aa][6])

		tampung_provinsi = []
		for q in range(len(data_x1)):
			tampung_provinsi.append(keluaran[aa][7])

		tampung_kota = []
		for r in range(len(data_x1)):
			tampung_kota.append(keluaran[aa][8])

		tampung_satuan = []
		for s in range(len(data_x1)):
			tampung_satuan.append(keluaran[aa][9])

		# tampung_sumber = []
		# hasil_split = keluaran[aa][10].split(",")
		# for t in range(len(data_x1)):
		# 	tampung_sumber.append(hasil_split[0])

		lala = []
		lala.append(tampung_nama_data)
		lala.append(data_x1)
		lala.append(data_y1)
		lala.append(tampung_nama_produk)
		lala.append(tampung_item)
		lala.append(tampung_negara)
		lala.append(tampung_provinsi)
		lala.append(tampung_kota)
		lala.append(tampung_satuan)
		# lala.append(tampung_sumber)
		
		df = pd.DataFrame(lala)
		df2 = df.T
		df3 = df2.rename({0:"Nama Data", 1:"Waktu", 2:"Nilai", 3:"Nama Produk", 4:"Item", 5:"Negara", 6:"Provinsi", 7:"Kota", 8:"Satuan"}, axis='columns')

		tampung_sementara.append(df3)

	df4 = pd.concat(tampung_sementara)

#####################################################################################################

	try:
		split_garis = rowx.split("|")
	except:
		return df4.to_csv("tesflask.csv", index = False)

	kumpulan1 = []
	kumpulan2 = []
	for v in split_garis:
		split_colon = v.split(":")
		split_colon_kiri = " ".join(split_colon[0].split("+"))

		if split_colon_kiri == "Waktu":
			split_colon_kanan =  split_colon[1]
			kumpulan1.append(split_colon_kiri)
			kumpulan1.append(split_colon_kanan)
			kumpulan2.append(kumpulan1)
			kumpulan1 = []

		else:
			split_colon_kanan = " ".join(split_colon[1].split("+"))
			kumpulan1.append(split_colon_kiri)
			kumpulan1.append(split_colon_kanan)
			kumpulan2.append(kumpulan1)
			kumpulan1 = []

	
	nama_data = []
	waktu = []
	nilai = []
	nama_produk = []
	item = []
	negara = []
	provinsi = []
	kota = []
	satuan = []
	for w in range(len(kumpulan2)):
		if kumpulan2[w][0] == "Nama Data":
			nama_data.append(kumpulan2[w])
		elif kumpulan2[w][0] == "Waktu":
			waktu.append(kumpulan2[w])
		elif kumpulan2[w][0] == "Nilai":
			nilai.append(kumpulan2[w])
		elif kumpulan2[w][0] == "Nama Produk":
			nama_produk.append(kumpulan2[w])
		elif kumpulan2[w][0] == "Item":
			item.append(kumpulan2[w])
		elif kumpulan2[w][0] == "Negara":
			negara.append(kumpulan2[w])
		elif kumpulan2[w][0] == "Provinsi":
			provinsi.append(kumpulan2[w])
		elif kumpulan2[w][0] == "Kota":
			kota.append(kumpulan2[w])
		elif kumpulan2[w][0] == "Satuan":
			satuan.append(kumpulan2[w])


	simpan4 = []
	simpan5 = [] 
	for z in range(len(provinsi)):
		simpan1 = provinsi[z][0] 
		simpan2 = "==" 
		simpan3 = provinsi[z][1]
		simpan4.append(simpan1)
		simpan4.append(simpan2)
		simpan4.append(simpan3)

		block = "(df4["+"'"+simpan4[0]+"'"+"]"+simpan4[1]+"'"+simpan4[2]+"')"
		simpan4 = []

		simpan5.append(block)


	jumlah = len(simpan5)

	if jumlah == 1:
		pertama = simpan5[0]
		pertamax = pertama[1:-1]
		lala = eval(pertamax)
		df5 = df4[lala]


	elif jumlah != 1:
		gabung = "|".join(simpan5)
		lala = eval(gabung)
		df5 = df4[lala]


		
	return df5.to_csv("tesflask.csv", index = False)

	# jadiin json


if __name__ == '__main__':
	app.run(host= '0.0.0.0',debug = True)











# if __name__ == '__main__':
#     app.run()


# @app.route("/lala")
# def home():
#     return 'Hello, Flask!'

#http://127.0.0.1:5000/listing?batas=0


	# for (z = 0; z < 10; z+1){
	# 	simpan1 = df4[provinsi[z][0]] == provinsi[z][1]
	# 	simpan2 = simpan1 + simpan2
	# }:
		