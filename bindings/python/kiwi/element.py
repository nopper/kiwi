from msgpack import dumps, loads

class KiwiElement(object):
	def __init__(self, graph, id=None):
		self.db = graph.db
		self.id = str(id)
		self.props = {}
		self.needs_update = False

	def get_id(self):
		return self.id

	def set(self, key, value):
		self.props[key] = value
		self.needs_update = True

	def get(self, key, default=None):
		try:
			return self.props[key]
		except:
			ret = self.db.get('P/%s/%s' % (key, self.get_id()))

			if ret is not None:
				try:
					value = loads(ret, use_list=True)
					self.props[key] = value
					return value
				except:
					pass
			
			return default

	def __str__(self):
		return "%s:%s" % (self.__class__.__name__, self.id)

	def __repr__(self):
		return str(self)

	def __del__(self):
		if self.needs_update:
			id = self.get_id()

			for key, value in self.props.iteritems():
				self.db.add('P/%s/%s' % (key, id), dumps(value))
