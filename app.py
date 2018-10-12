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
	columnx = request.args.get('column')
	rowx = request.args.get('row')	
	sortx = request.args.get('sort')
	limitx = request.args.get('limit')


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

	df4 = pd.concat(tampung_sementara) #df4 adalah dataframe seluruhnya tanpa filter


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################


	if columnx:
		potong_kolom = columnx.split("|")
		tampung = []
		for potong in potong_kolom:
			tampung.append(" ".join(potong.split("+")))

		df4 = df4[tampung]
		tampung = []

        #############################################################################################

		if rowx:
			split_garis_rowx = rowx.split("|") #['Provinsi:Aceh', 'Provinsi:Banten']
			 
			 
			kumpulan1 = []
			kumpulan2 = []
			for v in split_garis_rowx:
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

			#kumpulan2 = [['Provinsi', 'Aceh'], ['Provinsi', 'Banten']]
			#kumpulan2 = [[u'Provinsi', u'Aceh'], 
			# 			  [u'Nama Data', u'Persentase Penduduk Usia 5 Tahun ke Atas yang Pernah Mengakses Internet dalam 3 Bulan Terakhir Menurut Provinsi Tanpa Jenjang Pendidikan']]

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
				#provinsi = [["Provinsi", "Aceh"], ["Provinsi", "Banten"]]
				elif kumpulan2[w][0] == "Kota":
					kota.append(kumpulan2[w])
				elif kumpulan2[w][0] == "Satuan":
					satuan.append(kumpulan2[w])


			simpan4 = []
			simpan_provinsi = [] 
			for z in range(len(provinsi)):
				simpan1 = provinsi[z][0] 
				simpan2 = "==" 
				simpan3 = provinsi[z][1]
				simpan4.append(simpan1)
				simpan4.append(simpan2)
				simpan4.append(simpan3)
				block = "(df4["+"'"+simpan4[0]+"'"+"]"+simpan4[1]+"'"+simpan4[2]+"')"
				simpan4 = []
				simpan_provinsi.append(block)
				#simpan_provinsi = ["(df4['Provinsi']=='Aceh')", "(df4['Provinsi']=='Banten')"]
			
			simpan6 = []
			simpan_nama_data = [] 
			for z in range(len(nama_data)):
				simpan1 = nama_data[z][0] 
				simpan2 = "==" 
				simpan3 = nama_data[z][1]
				simpan6.append(simpan1)
				simpan6.append(simpan2)
				simpan6.append(simpan3)
				block = "(df4["+"'"+simpan6[0]+"'"+"]"+simpan6[1]+"'"+simpan6[2]+"')"
				simpan6 = []
				simpan_nama_data.append(block)
				#simpan_nama_data = #["(df4['Nama Data']=='Persentase Penduduk Usia 5 Tahun ke Atas yang Pernah Mengakses Internet dalam 3 Bulan Terakhir Menurut Provinsi Tanpa Jenjang Pendidikan')", 
									# "(df4['Nama Data']=='Persentase Penduduk Usia 5 Tahun ke Atas yang Pernah Mengakses Internet dalam 3 Bulan Terakhir Menurut Provinsi dengan Jenjang Pendidikan SD')"]

			simpan7 = []
			simpan_waktu = [] 
			for z in range(len(waktu)):
				simpan1 = waktu[z][0] 
				simpan2 = "==" 
				simpan3 = waktu[z][1]
				simpan7.append(simpan1)
				simpan7.append(simpan2)
				simpan7.append(simpan3)
				block = "(df4["+"'"+simpan7[0]+"'"+"]"+simpan7[1]+"'"+simpan7[2]+"')"
				simpan7 = []
				simpan_waktu.append(block)

			simpan8 = []
			simpan_nilai = [] 
			for z in range(len(nilai)):
				simpan1 = nilai[z][0] 
				simpan2 = "==" 
				simpan3 = nilai[z][1]
				simpan8.append(simpan1)
				simpan8.append(simpan2)
				simpan8.append(simpan3)
				block = "(df4["+"'"+simpan8[0]+"'"+"]"+simpan8[1]+"'"+simpan8[2]+"')"
				simpan8 = []
				simpan_nilai.append(block)

			simpan9 = []
			simpan_negara = [] 
			for z in range(len(negara)):
				simpan1 = negara[z][0] 
				simpan2 = "==" 
				simpan3 = negara[z][1]
				simpan9.append(simpan1)
				simpan9.append(simpan2)
				simpan9.append(simpan3)
				block = "(df4["+"'"+simpan9[0]+"'"+"]"+simpan9[1]+"'"+simpan9[2]+"')"
				simpan9 = []
				simpan_negara.append(block)

			#batas isinya ga muncul (kefilter habis)
			simpan10 = []
			simpan_satuan = [] 
			for z in range(len(satuan)):
				simpan1 = satuan[z][0] 
				simpan2 = "==" 
				simpan3 = satuan[z][1]
				simpan10.append(simpan1)
				simpan10.append(simpan2)
				simpan10.append(simpan3)
				block = "(df4["+"'"+simpan10[0]+"'"+"]"+simpan10[1]+"'"+simpan10[2]+"')"
				simpan10 = []
				simpan_satuan.append(block)

			simpan11 = []
			simpan_nama_produk = [] 
			for z in range(len(nama_produk)):
				simpan1 = nama_produk[z][0] 
				simpan2 = "==" 
				simpan3 = nama_produk[z][1]
				simpan11.append(simpan1)
				simpan11.append(simpan2)
				simpan11.append(simpan3)
				block = "(df4["+"'"+simpan11[0]+"'"+"]"+simpan11[1]+"'"+simpan11[2]+"')"
				simpan11 = []
				simpan_nama_produk.append(block)

			simpan12 = []
			simpan_item = [] 
			for z in range(len(item)):
				simpan1 = item[z][0] 
				simpan2 = "==" 
				simpan3 = item[z][1]
				simpan12.append(simpan1)
				simpan12.append(simpan2)
				simpan12.append(simpan3)
				block = "(df4["+"'"+simpan12[0]+"'"+"]"+simpan12[1]+"'"+simpan12[2]+"')"
				simpan12 = []
				simpan_nama_produk.append(block)

			simpan13 = []
			simpan_kota = [] 
			for z in range(len(kota)):
				simpan1 = kota[z][0] 
				simpan2 = "==" 
				simpan3 = kota[z][1]
				simpan13.append(simpan1)
				simpan13.append(simpan2)
				simpan13.append(simpan3)
				block = "(df4["+"'"+simpan13[0]+"'"+"]"+simpan13[1]+"'"+simpan13[2]+"')"
				simpan13 = []
				simpan_kota.append(block)


			jumlah = len(split_garis_rowx)

			if jumlah == 1:
				if simpan_provinsi:
					pertama = simpan_provinsi[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)
				elif simpan_nama_data:
					pertama = simpan_nama_data[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)
				elif simpan_waktu:
					pertama = simpan_waktu[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)
				elif simpan_nilai:
					pertama = simpan_nilai[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)
				elif simpan_negara:
					pertama = simpan_negara[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)
				elif simpan_satuan:
					pertama = simpan_satuan[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)
				elif simpan_nama_produk:
					pertama = simpan_nama_produk[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)
				elif simpan_item:
					pertama = simpan_item[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)
				elif simpan_kota:
					pertama = simpan_kota[0]
					lala = eval(pertama)
					df5 = df4[lala]
					df5.to_csv("tesflask.csv", index = False)


			elif jumlah != 1:


				def fungsi_or():
					if simpan_provinsi:
						gabung = "|".join(simpan_provinsi)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)

						

					elif simpan_nama_data:
						gabung = "|".join(simpan_nama_data)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif simpan_waktu:
						gabung = "|".join(simpan_waktu)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif simpan_nilai:
						gabung = "|".join(simpan_nilai)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif simpan_negara:
						gabung = "|".join(simpan_negara)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif simpan_satuan:
						gabung = "|".join(simpan_satuan)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif simpan_nama_produk:
						gabung = "|".join(simpan_nama_produk)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif simpan_item:
						gabung = "|".join(simpan_item)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)

					elif simpan_kota:
						gabung = "|".join(simpan_kota)
						lala = eval(gabung)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)
					

				def fungsi_and():			
						
					gabung1 = "|".join(simpan_provinsi)
					gabung2 = "("+gabung1+")"

					gabung3 = "|".join(simpan_nama_data)
					gabung4 = "("+gabung3+")"

					gabung5 = "|".join(simpan_waktu)
					gabung6 = "("+gabung5+")"

					gabung7 = "|".join(simpan_nilai)
					gabung8 = "("+gabung7+")"

					gabung9 = "|".join(simpan_negara)
					gabung10 = "("+gabung9+")"

					gabung11 = "|".join(simpan_satuan)
					gabung12 = "("+gabung11+")"

					gabung13 = "|".join(simpan_nama_produk)
					gabung14 = "("+gabung13+")"

					gabung15 = "|".join(simpan_item)
					gabung16 = "("+gabung15+")"

					gabung17 = "|".join(simpan_kota)
					gabung18 = "("+gabung17+")"

					tes = [] 
					for kum in kumpulan2:
						tes.append(kum[0])

					tes1 = np.array(tes)
					tes2 = np.unique(tes1) #tes2 = ["Nama Data", "Provinsi"]

					gabung_kumpul = []
					for t in tes2:
						if t == "Provinsi":
							gabung_kumpul.append(gabung2)
						elif t == "Nama Data":
							gabung_kumpul.append(gabung4)
						elif t == "Waktu":
							gabung_kumpul.append(gabung6)
						elif t == "Nilai":
							gabung_kumpul.append(gabung8)
						elif t == "Negara":
							gabung_kumpul.append(gabung10)
						elif t == "Satuan":
							gabung_kumpul.append(gabung12)
						elif t == "Nama Produk":
							gabung_kumpul.append(gabung14)
						elif t == "Item":
							gabung_kumpul.append(gabung16)
						elif t == "Kota":
							gabung_kumpul.append(gabung18)

					if len(tes2) == 2:
						gabung2 = gabung_kumpul[0]+"&"+gabung_kumpul[1]
						lala = eval(gabung2)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif len(tes2) == 3:
						gabung3 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]
						lala = eval(gabung3)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif len(tes2) == 4:
						gabung4 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]
						lala = eval(gabung4)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif len(tes2) == 5:
						gabung5 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]
						lala = eval(gabung5)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif len(tes2) == 6:
						gabung6 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]+"&"+gabung_kumpul[5]
						lala = eval(gabung6)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif len(tes2) == 7:
						gabung7 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]+"&"+gabung_kumpul[5]+"&"+gabung_kumpul[6]
						lala = eval(gabung7)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif len(tes2) == 8:
						gabung8 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]+"&"+gabung_kumpul[5]+"&"+gabung_kumpul[6]+"&"+gabung_kumpul[7]
						lala = eval(gabung8)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					elif len(tes2) == 9:
						gabung9 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]+"&"+gabung_kumpul[5]+"&"+gabung_kumpul[6]+"&"+gabung_kumpul[7]+"&"+gabung_kumpul[8]
						lala = eval(gabung9)
						df5 = df4[lala]

						if sortx:
							split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

							sort_kiri = []
							sort_kanan = []
							for split_sort in split_garis_sortx:
								split_colon_sort = split_sort.split(":")
								sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
								sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

							sort_kanan_rubah_simpan = []
							for sort_kanan_rubah in sort_kanan:
								if sort_kanan_rubah == "asc":
									sort_kanan_rubah = True
								elif sort_kanan_rubah == "desc":
									sort_kanan_rubah = False

								sort_kanan_rubah_simpan.append(sort_kanan_rubah)

							df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

							sort_kiri = []
							sort_kanan = []
							sort_kanan_rubah_simpan = []


							if limitx:
								df5 = df5.head(int(limitx))
								df5.to_csv("tesflask.csv", index = False)

							else:
								df5.to_csv("tesflask.csv", index = False)

						
						elif limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)


						else:
							df5.to_csv("tesflask.csv", index = False)


					
					# return df5.to_csv("tesflask.csv", index = False)


				cekBeda = True
				for y in range(0, len(kumpulan2)):
					if kumpulan2[y][0] != kumpulan2[y-1][0]:
						cekBeda = False

				if cekBeda == True:
					fungsi_or()
				else:
					fungsi_and()

				kumpulan2 = []

		elif sortx:
			
			split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

			sort_kiri = []
			sort_kanan = []
			for split_sort in split_garis_sortx:
				split_colon_sort = split_sort.split(":")
				sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
				sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

			sort_kanan_rubah_simpan = []
			for sort_kanan_rubah in sort_kanan:
				if sort_kanan_rubah == "asc":
					sort_kanan_rubah = True
				elif sort_kanan_rubah == "desc":
					sort_kanan_rubah = False

				sort_kanan_rubah_simpan.append(sort_kanan_rubah)

			df4 = df4.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

			sort_kiri = []
			sort_kanan = []
			sort_kanan_rubah_simpan = []


			if limitx:
				df4 = df4.head(int(limitx))
				df4.to_csv("tesflask.csv", index = False)

			else:
				df4.to_csv("tesflask.csv", index = False)

		
		elif limitx:
			df4 = df4.head(int(limitx))
			df4.to_csv("tesflask.csv", index = False)


		else:
			df4.to_csv("tesflask.csv", index = False)

			

#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################

	elif rowx:
		split_garis_rowx = rowx.split("|") #['Provinsi:Aceh', 'Provinsi:Banten']
			 
	
		kumpulan1 = []
		kumpulan2 = []
		for v in split_garis_rowx:
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

		#kumpulan2 = [['Provinsi', 'Aceh'], ['Provinsi', 'Banten']]
		#kumpulan2 = [[u'Provinsi', u'Aceh'], 
		# 			  [u'Nama Data', u'Persentase Penduduk Usia 5 Tahun ke Atas yang Pernah Mengakses Internet dalam 3 Bulan Terakhir Menurut Provinsi Tanpa Jenjang Pendidikan']]

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
			#provinsi = [["Provinsi", "Aceh"], ["Provinsi", "Banten"]]
			elif kumpulan2[w][0] == "Kota":
				kota.append(kumpulan2[w])
			elif kumpulan2[w][0] == "Satuan":
				satuan.append(kumpulan2[w])


		simpan4 = []
		simpan_provinsi = [] 
		for z in range(len(provinsi)):
			simpan1 = provinsi[z][0] 
			simpan2 = "==" 
			simpan3 = provinsi[z][1]
			simpan4.append(simpan1)
			simpan4.append(simpan2)
			simpan4.append(simpan3)
			block = "(df4["+"'"+simpan4[0]+"'"+"]"+simpan4[1]+"'"+simpan4[2]+"')"
			simpan4 = []
			simpan_provinsi.append(block)
			#simpan_provinsi = ["(df4['Provinsi']=='Aceh')", "(df4['Provinsi']=='Banten')"]
		
		simpan6 = []
		simpan_nama_data = [] 
		for z in range(len(nama_data)):
			simpan1 = nama_data[z][0] 
			simpan2 = "==" 
			simpan3 = nama_data[z][1]
			simpan6.append(simpan1)
			simpan6.append(simpan2)
			simpan6.append(simpan3)
			block = "(df4["+"'"+simpan6[0]+"'"+"]"+simpan6[1]+"'"+simpan6[2]+"')"
			simpan6 = []
			simpan_nama_data.append(block)
			#simpan_nama_data = #["(df4['Nama Data']=='Persentase Penduduk Usia 5 Tahun ke Atas yang Pernah Mengakses Internet dalam 3 Bulan Terakhir Menurut Provinsi Tanpa Jenjang Pendidikan')", 
								# "(df4['Nama Data']=='Persentase Penduduk Usia 5 Tahun ke Atas yang Pernah Mengakses Internet dalam 3 Bulan Terakhir Menurut Provinsi dengan Jenjang Pendidikan SD')"]

		simpan7 = []
		simpan_waktu = [] 
		for z in range(len(waktu)):
			simpan1 = waktu[z][0] 
			simpan2 = "==" 
			simpan3 = waktu[z][1]
			simpan7.append(simpan1)
			simpan7.append(simpan2)
			simpan7.append(simpan3)
			block = "(df4["+"'"+simpan7[0]+"'"+"]"+simpan7[1]+"'"+simpan7[2]+"')"
			simpan7 = []
			simpan_waktu.append(block)

		simpan8 = []
		simpan_nilai = [] 
		for z in range(len(nilai)):
			simpan1 = nilai[z][0] 
			simpan2 = "==" 
			simpan3 = nilai[z][1]
			simpan8.append(simpan1)
			simpan8.append(simpan2)
			simpan8.append(simpan3)
			block = "(df4["+"'"+simpan8[0]+"'"+"]"+simpan8[1]+"'"+simpan8[2]+"')"
			simpan8 = []
			simpan_nilai.append(block)

		simpan9 = []
		simpan_negara = [] 
		for z in range(len(negara)):
			simpan1 = negara[z][0] 
			simpan2 = "==" 
			simpan3 = negara[z][1]
			simpan9.append(simpan1)
			simpan9.append(simpan2)
			simpan9.append(simpan3)
			block = "(df4["+"'"+simpan9[0]+"'"+"]"+simpan9[1]+"'"+simpan9[2]+"')"
			simpan9 = []
			simpan_negara.append(block)

		#batas isinya ga muncul (kefilter habis)
		simpan10 = []
		simpan_satuan = [] 
		for z in range(len(satuan)):
			simpan1 = satuan[z][0] 
			simpan2 = "==" 
			simpan3 = satuan[z][1]
			simpan10.append(simpan1)
			simpan10.append(simpan2)
			simpan10.append(simpan3)
			block = "(df4["+"'"+simpan10[0]+"'"+"]"+simpan10[1]+"'"+simpan10[2]+"')"
			simpan10 = []
			simpan_satuan.append(block)

		simpan11 = []
		simpan_nama_produk = [] 
		for z in range(len(nama_produk)):
			simpan1 = nama_produk[z][0] 
			simpan2 = "==" 
			simpan3 = nama_produk[z][1]
			simpan11.append(simpan1)
			simpan11.append(simpan2)
			simpan11.append(simpan3)
			block = "(df4["+"'"+simpan11[0]+"'"+"]"+simpan11[1]+"'"+simpan11[2]+"')"
			simpan11 = []
			simpan_nama_produk.append(block)

		simpan12 = []
		simpan_item = [] 
		for z in range(len(item)):
			simpan1 = item[z][0] 
			simpan2 = "==" 
			simpan3 = item[z][1]
			simpan12.append(simpan1)
			simpan12.append(simpan2)
			simpan12.append(simpan3)
			block = "(df4["+"'"+simpan12[0]+"'"+"]"+simpan12[1]+"'"+simpan12[2]+"')"
			simpan12 = []
			simpan_nama_produk.append(block)

		simpan13 = []
		simpan_kota = [] 
		for z in range(len(kota)):
			simpan1 = kota[z][0] 
			simpan2 = "==" 
			simpan3 = kota[z][1]
			simpan13.append(simpan1)
			simpan13.append(simpan2)
			simpan13.append(simpan3)
			block = "(df4["+"'"+simpan13[0]+"'"+"]"+simpan13[1]+"'"+simpan13[2]+"')"
			simpan13 = []
			simpan_kota.append(block)


		jumlah = len(split_garis_rowx)

		if jumlah == 1:
			if simpan_provinsi:
				pertama = simpan_provinsi[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)
			elif simpan_nama_data:
				pertama = simpan_nama_data[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)
			elif simpan_waktu:
				pertama = simpan_waktu[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)
			elif simpan_nilai:
				pertama = simpan_nilai[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)
			elif simpan_negara:
				pertama = simpan_negara[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)
			elif simpan_satuan:
				pertama = simpan_satuan[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)
			elif simpan_nama_produk:
				pertama = simpan_nama_produk[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)
			elif simpan_item:
				pertama = simpan_item[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)
			elif simpan_kota:
				pertama = simpan_kota[0]
				lala = eval(pertama)
				df5 = df4[lala]
				df5.to_csv("tesflask.csv", index = False)


		elif jumlah != 1:


			def fungsi_or():
				if simpan_provinsi:
					gabung = "|".join(simpan_provinsi)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)

				elif simpan_nama_data:
					gabung = "|".join(simpan_nama_data)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif simpan_waktu:
					gabung = "|".join(simpan_waktu)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif simpan_nilai:
					gabung = "|".join(simpan_nilai)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif simpan_negara:
					gabung = "|".join(simpan_negara)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif simpan_satuan:
					gabung = "|".join(simpan_satuan)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif simpan_nama_produk:
					gabung = "|".join(simpan_nama_produk)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif simpan_item:
					gabung = "|".join(simpan_item)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif simpan_kota:
					gabung = "|".join(simpan_kota)
					lala = eval(gabung)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


			def fungsi_and():			
					
				gabung1 = "|".join(simpan_provinsi)
				gabung2 = "("+gabung1+")"

				gabung3 = "|".join(simpan_nama_data)
				gabung4 = "("+gabung3+")"

				gabung5 = "|".join(simpan_waktu)
				gabung6 = "("+gabung5+")"

				gabung7 = "|".join(simpan_nilai)
				gabung8 = "("+gabung7+")"

				gabung9 = "|".join(simpan_negara)
				gabung10 = "("+gabung9+")"

				gabung11 = "|".join(simpan_satuan)
				gabung12 = "("+gabung11+")"

				gabung13 = "|".join(simpan_nama_produk)
				gabung14 = "("+gabung13+")"

				gabung15 = "|".join(simpan_item)
				gabung16 = "("+gabung15+")"

				gabung17 = "|".join(simpan_kota)
				gabung18 = "("+gabung17+")"

				
				tes = [] 
				for kum in kumpulan2:
					tes.append(kum[0])

				tes1 = np.array(tes)
				tes2 = np.unique(tes1) #tes2 = ["Nama Data", "Provinsi"]

				gabung_kumpul = []
				for t in tes2:
					if t == "Provinsi":
						gabung_kumpul.append(gabung2)
					elif t == "Nama Data":
						gabung_kumpul.append(gabung4)
					elif t == "Waktu":
						gabung_kumpul.append(gabung6)
					elif t == "Nilai":
						gabung_kumpul.append(gabung8)
					elif t == "Negara":
						gabung_kumpul.append(gabung10)
					elif t == "Satuan":
						gabung_kumpul.append(gabung12)
					elif t == "Nama Produk":
						gabung_kumpul.append(gabung14)
					elif t == "Item":
						gabung_kumpul.append(gabung16)
					elif t == "Kota":
						gabung_kumpul.append(gabung18)

				if len(tes2) == 2:
					gabung2 = gabung_kumpul[0]+"&"+gabung_kumpul[1]
					lala = eval(gabung2)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif len(tes2) == 3:
					gabung3 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]
					lala = eval(gabung3)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif len(tes2) == 4:
					gabung4 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]
					lala = eval(gabung4)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif len(tes2) == 5:
					gabung5 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]
					lala = eval(gabung5)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif len(tes2) == 6:
					gabung6 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]+"&"+gabung_kumpul[5]
					lala = eval(gabung6)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif len(tes2) == 7:
					gabung7 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]+"&"+gabung_kumpul[5]+"&"+gabung_kumpul[6]
					lala = eval(gabung7)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif len(tes2) == 8:
					gabung8 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]+"&"+gabung_kumpul[5]+"&"+gabung_kumpul[6]+"&"+gabung_kumpul[7]
					lala = eval(gabung8)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				elif len(tes2) == 9:
					gabung9 = gabung_kumpul[0]+"&"+gabung_kumpul[1]+"&"+gabung_kumpul[2]+"&"+gabung_kumpul[3]+"&"+gabung_kumpul[4]+"&"+gabung_kumpul[5]+"&"+gabung_kumpul[6]+"&"+gabung_kumpul[7]+"&"+gabung_kumpul[8]
					lala = eval(gabung9)
					df5 = df4[lala]

					if sortx:
						split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

						sort_kiri = []
						sort_kanan = []
						for split_sort in split_garis_sortx:
							split_colon_sort = split_sort.split(":")
							sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
							sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))

						sort_kanan_rubah_simpan = []
						for sort_kanan_rubah in sort_kanan:
							if sort_kanan_rubah == "asc":
								sort_kanan_rubah = True
							elif sort_kanan_rubah == "desc":
								sort_kanan_rubah = False

							sort_kanan_rubah_simpan.append(sort_kanan_rubah)

						df5 = df5.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

						sort_kiri = []
						sort_kanan = []
						sort_kanan_rubah_simpan = []


						if limitx:
							df5 = df5.head(int(limitx))
							df5.to_csv("tesflask.csv", index = False)

						else:
							df5.to_csv("tesflask.csv", index = False)

					
					elif limitx:
						df5 = df5.head(int(limitx))
						df5.to_csv("tesflask.csv", index = False)


					else:
						df5.to_csv("tesflask.csv", index = False)


				
				


			cekBeda = True
			for y in range(0, len(kumpulan2)):
				if kumpulan2[y][0] != kumpulan2[y-1][0]:
					cekBeda = False

			if cekBeda == True:
				df5 = fungsi_or()
			else:
				df5 = fungsi_and()

			kumpulan2 = []

#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################

	elif sortx:
		split_garis_sortx = sortx.split("|") #['Provinsi:ASC', 'Waktu:ASC', 'Nama Data:ASC']

		sort_kiri = []
		sort_kanan = []
		for split_sort in split_garis_sortx:
			split_colon_sort = split_sort.split(":")
			sort_kiri.append(" ".join(split_colon_sort[0].split("+")))
			sort_kanan.append(" ".join(split_colon_sort[1].lower().split("+")))


		sort_kanan_rubah_simpan = []
		for sort_kanan_rubah in sort_kanan:
			if sort_kanan_rubah == "asc":
				sort_kanan_rubah = True
			elif sort_kanan_rubah == "desc":
				sort_kanan_rubah = False

			sort_kanan_rubah_simpan.append(sort_kanan_rubah)


		df5 = df4.sort_values(sort_kiri, ascending = sort_kanan_rubah_simpan)

		sort_kiri = []
		sort_kanan = []
		sort_kanan_rubah_simpan = []

		if limitx:
			df5 = df5.head(int(limitx))
			df5.to_csv("tesflask.csv", index = False)

		else:
			df5.to_csv("tesflask.csv", index = False)


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################

	elif limitx:
		df5 = df4.head(int(limitx))
		df5.to_csv("tesflask.csv", index = False)

#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################

	else:
		df4.to_csv("tesflask.csv", index = False)


	return "Berhasil"
	

if __name__ == '__main__':
	app.run(host= '0.0.0.0',debug = True)






	# no = []
	# for t in range(len(df4)):
	# 	no.append(t+1)

	# df4["No"] = no

	# #Pindah posisi kolom No dari di akhir menjadi di awal
	# rubah1 = df4['No']
	# df4.drop(labels=['No'], axis=1,inplace = True)
	# df4.insert(0, 'No', rubah1)











	# @app.route('/login', methods=['POST', 'GET'])
# def login():
#     error = None
#     if request.method == 'POST':
#         if valid_login(request.form['username'],
#                        request.form['password']):
#             return log_the_user_in(request.form['username'])
#         else:
#             error = 'Invalid username/password'
#     # the code below is executed if the request method
#     # was GET or the credentials were invalid
#     return render_template('login.html', error=error)