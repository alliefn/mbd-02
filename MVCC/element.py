'''
	Kelas untuk merepresentasikan Elemen
	Terdiri dari key, val, writeTS, readTS, dan version
'''

class Element:
	def __init__(self, key, val, readTS, writeTS, version):
		self.key = key
		self.val = val
		self.readTS = readTS
		self.writeTS = writeTS
		self.version = version