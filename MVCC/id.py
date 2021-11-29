'''
	Kelas untuk merepresentasikan ID
	Terdiri dari key dan ID
'''

class ID:
	def __init__(self, key, id, nilai):
		self.key = id
		self.id = key
		self.nilai = nilai

	def getKey(self):
		return self.key

	def getID(self):
		return self.id

	def getNilai(self):
		return self.nilai