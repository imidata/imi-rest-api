import psycopg2
from operator import itemgetter
from datetime import datetime
import os
import shutil
import subprocess


class ImiModel(object):

	def __init__(self, database_url=None):
		if not database_url:
			raise Exception("database_url is required")		
		self.conn = psycopg2.connect(database_url)

		# list of the different geographic extents we can use to group data from largest to smallest
		self.geo_group_by_levels = ["nation","region","state","msa","county","postal code", "postal_code"]

		# list of the different segmentation extents we can use to group data from largest to smallest
		self.seg_group_by_levels = [ "division", "major", "industry", "code" ]

	def close(self):
		self.conn.close()


	def valid_geo_group_by(self, group_by=None ):
		if group_by in self.geo_group_by_levels:
			return True
		return False

	def valid_seg_group_by(self, group_by=None ):
		if group_by in ["division","code"]:
			return True
		return False

	def valid_seg_type(self, seg_type=None ):
		if seg_type in ["naics","sic"]:
			return True
		return False

	def valid_products(self, products=None ):
		"""Check to see if a product list is valid"""

		if type(products) == str:
			products = [products]

		if products and len(products) > 0: 
			all_good = True
			for p in products:
				cur = self.conn.cursor()
				cur.execute("""select 
					*
					from ratios r
					where (r.product_id=%s) limit 1
				""", (p, ) )

				row = cur.fetchone()
				if row is None:
					all_good = False
					break
				cur.close()

			return all_good

		return False


	def valid_geo_filter(self, geo=None ):
		"""Check to see if a geo filter list is valid"""

		if geo is None or geo == [] or geo == [{}]:
			return True

		if type(geo) is not type([]):
			return False

		all_good = True
		cur = self.conn.cursor()

		for g in geo:
			if type(g) is not type({}):
				return False
			f = self.geo_filter_to_sql(g)

			if f is None:
				all_good = False
				break

			cur.execute("""select * from geo l where ({}) limit 1""".format(f) )
			row = cur.fetchone()
			if row is None:
				all_good = False
				break

		cur.close()
		return all_good

	def geo_filter_to_sql(self, f=None ):
		"""Take one part of a geo filter python object array and parse it to SQL. Returns none if the filter is not valid."""

		if f is None or f == {} or not f:
			return ""

		# check that all keys are valid
		for key in f.keys():
			if key not in self.geo_group_by_levels:
				return None

		filter_query = None
		cur = self.conn.cursor()

		if "nation" in f and "county" in f:
			filter_query = cur.mogrify("(l.nation=%s and l.state=%s and l.county=%s)", (f["nation"],f["state"],f["county"]) )
		elif "nation" in f and "msa" in f:
			filter_query = cur.mogrify("(l.nation=%s and l.msa=%s)", (f["nation"],f["msa"]) )
		elif "nation" in f and "state" in f:
			filter_query = cur.mogrify("(l.nation=%s and l.state=%s)", (f["nation"],f["state"]) )
		elif "nation" in f and "region" in f:
			filter_query = cur.mogrify("(l.nation=%s and l.region=%s)", (f["nation"],f["region"]) )
		elif "nation" in f:
			filter_query = cur.mogrify("(l.nation=%s)", (f["nation"],) )	

		cur.close()
		return filter_query

	def valid_duns(self, duns=None ):
		if duns is None:
			return False
		try:
			duns = str(duns)
		except:
			return False
		if len(duns) != 9:
			return False
		return True


	def valid_seg_filter(self, seg=None ):
		"""Check to see if a seg filter list is valid"""

		if seg is None or seg == {}:
			return True

		if type(seg) is not type({}):
			return False

		if "seg_type" not in seg or "filter" not in seg:
			return False

		seg_type = seg["seg_type"]
		seg_filter = seg["filter"]

		if not self.valid_seg_type(seg_type ):
			return False

		if type(seg_filter) != type("") and type(seg_filter) != type([]) and seg_filter is not None:
			return False

		if type(seg_filter) == type("") or seg_filter == None:
			seg_filter = [seg_filter]

		all_good = True
		for f in seg_filter:

			if f is None or f == "" or not f:
				continue

			filter_query = None
			cur = self.conn.cursor()

			# is this one a range query?
			if ":" in f:
				parts = f.split(":")
				if len(parts) != 2:
					all_good = False
					break
				else:
					if seg_type == "naics":
						#if group_by == "division":
						#	filter_query = cur.mogrify("(s.parent>=%s and s.parent<=%s)", (parts[0],parts[1]) )
						#else:
						filter_query = cur.mogrify("(s.naics>=%s and s.naics<=%s)", (parts[0],parts[1]) )
					else:
						#if group_by == "division":
						#	filter_query = cur.mogrify("(s.parent>=%s and s.parent<=%s)", (parts[0],parts[1]) )
						#else:
						filter_query = cur.mogrify("(s.sic>=%s and s.sic<=%s)", (parts[0],parts[1]) )
			else:
				if seg_type == "naics":
					#if group_by == "division":
					#	filter_query = cur.mogrify("(s.parent=%s)", (f,) )
					#else:
					filter_query = cur.mogrify("(s.naics=%s)", (f,) )
				else:
					#if group_by == "division":
					#	filter_query = cur.mogrify("(s.parent=%s)", (f,) )
					#else:
					filter_query = cur.mogrify("(s.sic=%s)", (f,) )

			if seg_type == "naics":
				cur.execute("""select * from naics s where ({}) limit 1""".format(filter_query) )
			else:
				cur.execute("""select * from sic s where ({}) limit 1""".format(filter_query) )

			row = cur.fetchone()
			if row is None:
				all_good = False
				break


			cur.close()


		return all_good



	def min_extent( self, geo_filter=None ):
		"""Given a geo filter return the extent which would contain the smallest level (most detailed) geo filter"""
		if not self.valid_geo_filter(geo_filter):
			raise Exception("geo_filter {}".format(geo_filter))

		extents = []
		for geo in geo_filter:
			for h in geo:
				if h not in extents:
					extents.append(h)

		if "postal code" in extents:
			return "postal code"
		elif "postal_code" in extents:
			return "postal_code"
		elif "county" in extents:
			return "county"
		elif "msa" in extents:
			return "msa"
		elif "state" in extents:
			return "state"
		elif "region" in extents:
			return "region"
		else:
			return "nation"



	def build_geo_filter_where_query( self, geo_filter=None ):
		"""Convert geo filter object into a valid sql where query"""
		if not self.valid_geo_filter(geo_filter):
			raise Exception("geo_filter {}".format(geo_filter))

		filter_query = ""
		for f in geo_filter:
			if filter_query != "":
				filter_query += " or "
			filter_query += self.geo_filter_to_sql(f)
		return filter_query

	def build_seg_filter_where_query( self, seg_filter=None ):
		"""Convert seg filter object into a valid sql where query"""

		if not self.valid_seg_filter(seg_filter):
			raise Exception("seg_filter {}".format(seg_filter))

		filter_query = ""

		if seg_filter is None:	
			return "1=1"

		seg_type=seg_filter['seg_type']
		seg_filter=seg_filter['filter']
		cur = self.conn.cursor()

		if type(seg_filter) == type("") or seg_filter == None:
			seg_filter = [seg_filter]    

		for f in seg_filter:
			if filter_query != "":
				filter_query += " or "
			if ":" in f:
				parts = f.split(":")
				if seg_type == "naics":
					filter_query += cur.mogrify("(l.naics>=%s and l.naics<=%s)",(parts[0],parts[1]))
				elif seg_type=="sic":
					filter_query += cur.mogrify("(l.sic>=%s and l.sic<=%s)",(parts[0],parts[1]))
			else:
				if seg_type == "naics":
					filter_query += cur.mogrify("(l.naics=%s)",(f,))
				elif seg_type=="sic":
					filter_query += cur.mogrify("(l.sic=%s)",(f,))
		
		return filter_query


	def fingerprint( self  ):
		"""return the GIT version number of the model from the database used as a fingerprint to tell which version data comes from"""
		cur = self.conn.cursor()
		cur.execute("select version from version;")
		version = cur.fetchone()[0]
		cur.close()
		return version


		# build the geo part of the where query
		geo_query = self.build_geo_filter_where_query(geo_filter=geo_filter)

	def geo_filter_to_words( self, geo_filter=None ):
		#if not self.valid_geo_filter(geo_filter):
		#	raise ImiInvalidInputError

		if geo_filter is None or geo_filter == {}:
			return "North America"

		words = ""
		if "nation" in geo_filter:
			words += geo_filter["nation"]
		if "region" in geo_filter:
			if words != "":
				words += " - "
			words += geo_filter["region"]
		if "state" in geo_filter:
			if words != "":
				words += " - "
			words += geo_filter["state"]
		if "msa" in geo_filter:
			if words != "":
				words += " - "
			words += geo_filter["msa"]
		if "county" in geo_filter:
			if words != "":
				words += " - "
			words += geo_filter["county"]

		return words


	def geo_query( self, group_by=None, geo_filter=None, seg_filter=None, products=None  ):
		"""Show demand and employee count totals for given inputs"""
		if not self.valid_geo_group_by(group_by):
			raise Exception("group_by {}".format(group_by))
		if not self.valid_geo_filter(geo_filter):
			raise Exception("geo_filter {}".format(geo_filter))
		if not self.valid_products(products):
			raise Exception("products {}".format(products))
		if not self.valid_seg_filter(seg_filter):
			raise Exception("seg_filter {}".format(seg_filter))

		# build the geo part of the where query
		geo_query = self.build_geo_filter_where_query(geo_filter=geo_filter)
		seg_query = self.build_seg_filter_where_query(seg_filter=seg_filter)

		geo_columns = "l.nation"
		header = [ group_by.capitalize(),"Demand", "Companies" ]
		if group_by == "postal_code":
			geo_columns = "l.nation,l.state,l.county,lpad(l.state_fips,2,'0') || lpad(l.county_fips,3,'0'),l.postal_code"
			header = [ "Nation","State","County","County FIPS","Postal Code","Demand","Companies" ] 
			#geo_columns = "l.nation,l.state,l.county,l.postal_code"
			#header = [ "Nation","State","County","Postal Code","Demand" ] 
		elif group_by == "county":
			geo_columns = "l.nation,l.state,l.county,lpad(l.state_fips,2,'0') || lpad(l.county_fips,3,'0')"
			header = [ "Nation","State","County","FIPS","Demand","Companies" ] 
			#geo_columns = "l.nation,l.state,l.county"
			#header = [ "Nation","State","County","Demand" ] 
		elif group_by == "msa":
			geo_columns = "l.nation,l.msa"
			header = [ "Nation","MSA","Demand","Companies" ] 
		elif group_by == "state":
			geo_columns = "l.nation,l.state"
			header = [ "Nation","State","Demand","Companies" ] 
		elif group_by == "region":
			geo_columns = "l.nation,l.region"
			header = [ "Nation","Region","Demand","Companies" ] 

		# there are cases where the geo filter requires the min extent table rather than the group by table. if you want to group by msa by filter by specific counties for example
		extent = self.min_extent( geo_filter )

		if extent == "nation" and group_by in ["region","state","msa","county","postal code", "postal_code"]:
			extent = group_by
		elif extent == "region" and group_by in ["state","msa","county","postal code", "postal_code"]:			
			extent = group_by
		elif extent == "state" and group_by in ["msa","county","postal code", "postal_code"]:			
			extent = group_by
		elif extent == "msa" and group_by in ["county","postal code", "postal_code"]:			
			extent = group_by
		elif extent == "county" and group_by in ["postal code", "postal_code"]:			
			extent = group_by

		cur = self.conn.cursor()
		"""cur.execute('''
			select 
			{},
			round(sum(l.employees*r.ratio)) as demand,
			count(*) as companies
			from locations l
			inner join (select sic, sum(ratio) as ratio
			from ratios r 
			where product_id=ANY(%s)
			group by sic) as r on r.sic=l.sic
			inner join geo g on g.id=l.geo_id
			where ({}) and ({})
			group by {}
			order by demand desc
			'''.format(geo_columns,geo_query,seg_query,geo_columns), (products,))
		"""
		cur.execute('''
			select 
			{},
			round(sum(l.employees*r.ratio)) as demand,
			sum(companies) as companies
			from locations_{} l
			inner join (select sic, sum(ratio) as ratio
			from ratios r 
			where product_id=ANY(%s)
			group by sic) as r on r.sic=l.sic
			where ({}) and ({})
			group by {}
			order by demand desc
			'''.format(geo_columns,extent,geo_query,seg_query,geo_columns), (products,))
		results = []
		total_demand = 0
		total_companies = 0
		for row in cur:
			results.append( row )
			total_companies += int(row[-1])
			total_demand += int(row[-2])

		cur.close()

		to_return = {
			"header":header,
			"results":results,
			"total_demand": total_demand,
			"total_companies": total_companies
		}

		return to_return


	

	def seg_query( self, group_by=None, seg_type=None, geo_filter=None, seg_filter=None, products=None  ):
		"""Show demand and employee count totals for given inputs"""
		if not self.valid_seg_type(seg_type):
			raise Exception("seg_type {}".format(seg_type))
		if not self.valid_seg_group_by(group_by):
			raise Exception("group_by {}".format(group_by))
		if not self.valid_geo_filter(geo_filter):
			raise Exception("geo_filter {}".format(geo_filter))
		if not self.valid_products(products):
			raise Exception("products {}".format(products))
		if not self.valid_seg_filter(seg_filter):
			raise Exception("seg_filter {}".format(seg_filter))

		# build the geo part of the where query
		geo_query = self.build_geo_filter_where_query(geo_filter=geo_filter)
		seg_query = self.build_seg_filter_where_query(seg_filter=seg_filter)

		extent = self.min_extent( geo_filter )

		cur = self.conn.cursor()

		if group_by == "division":
			'''cur.execute("""
				select s.parent,s.parent_description, 
				round(sum(l.employees*r.ratio)) as demand,
				count(*) as companies
				from locations l
				inner join (select sic, sum(ratio) as ratio
					from ratios r 
					where product_id=ANY(%s)
					group by sic) as r on r.sic=l.sic
				inner join geo g on g.id=l.geo_id
				left join {} s on s.{}=l.{}
				where ({}) and ({})
				group by s.parent, s.parent_description
				order by demand desc
			""".format(seg_type,seg_type, seg_type, geo_query, seg_query), (products, ) )
			'''
			cur.execute("""
				select s.parent,s.parent_description, 
				round(sum(l.employees*r.ratio)) as demand,
				sum(companies) as companies
				from locations_{} l
				inner join (select sic, sum(ratio) as ratio
					from ratios r 
					where product_id=ANY(%s)
					group by sic) as r on r.sic=l.sic
				left join {} s on s.{}=l.{}
				where ({}) and ({})
				group by s.parent, s.parent_description
				order by demand desc
			""".format(extent,seg_type,seg_type, seg_type, geo_query, seg_query), (products, ) )
		else:
			'''cur.execute("""
				select l.{},s.description,
				round(sum(l.employees*r.ratio)) as demand,
				count(*) as companies
				from locations l
				inner join (select sic, sum(ratio) as ratio
					from ratios r 
					where product_id=ANY(%s)
					group by sic) as r on r.sic=l.sic
				inner join geo g on g.id=l.geo_id
				left join {} s on s.{}=l.{}
				where ({}) and ({})
				group by l.{},s.description
				order by demand desc
			""".format(seg_type, seg_type, seg_type, seg_type, geo_query, seg_query, seg_type), (products, ) )
			'''
			cur.execute("""
				select l.{},s.description,
				round(sum(l.employees*r.ratio)) as demand,
				sum(companies) as companies
				from locations_{} l
				inner join (select sic, sum(ratio) as ratio
					from ratios r 
					where product_id=ANY(%s)
					group by sic) as r on r.sic=l.sic
				left join {} s on s.{}=l.{}
				where ({}) and ({})
				group by l.{},s.description
				order by demand desc
			""".format(seg_type,extent, seg_type, seg_type, seg_type, geo_query, seg_query, seg_type), (products, ) )

		results = []
		header = [ seg_type.upper(), "Description", "Demand", "Companies" ]
		total_demand = 0
		total_companies = 0
		for row in cur:
			results.append( row )
			total_demand += int(row[-2])
			total_companies += int(row[-1])

		cur.close()

		to_return = {
			"header":header,
			"results":results,
			"total_demand": total_demand,
			"total_companies": total_companies
		}

		return to_return


	def prospects( self, geo_filter=None, seg_filter=None, products=None, limit=100 ):
		"""list of the top prospects for a product"""
		if not self.valid_geo_filter(geo_filter):
			raise Exception("geo_filter {}".format(geo_filter))
		if not self.valid_products(products):
			raise Exception("products {}".format(products))
		if not self.valid_seg_filter(seg_filter):
			raise Exception("seg_filter {}".format(seg_filter))

		if not type(limit) == type(0):
			raise Exception("limit {}".format(limit))

		geo_query = self.build_geo_filter_where_query(geo_filter=geo_filter)
		seg_query = self.build_seg_filter_where_query(seg_filter=seg_filter)

		cur = self.conn.cursor()
		cur.execute( """
			select
			l.duns, l.name, l.url, l.employees, l.sic, s.description, l.naics, n.description,
			l.sales, g.nation, g.region, g.state, g.msa, g.county, g.postal_code, l.lon, l.lat,
			round(l.employees*r.ratio) as demand
			from
			locations l
			inner join (select sic, sum(ratio) as ratio
				from ratios r 
				where product_id=ANY(%s)
				group by sic) as r on r.sic=l.sic
			inner join geo g on g.id=l.geo_id
			left join sic s on s.sic=l.sic
			left join naics n on n.naics=l.naics
			where ({}) and ({})
			order by demand desc
			limit {}
		""".format(geo_query.replace("l.","g."),seg_query,limit), (products,))

		results = []
		header = ["DUNS","Name","URL","Employees","SIC","SIC Description", "NAICS", "NAICS Description", "Sales", "Country","Region","State","MSA","County","Postal Code","Longitude","Latitude", "Demand" ]
		total_demand = 0
		total_companies = 0
		for row in cur:
			results.append( row )
			total_demand += int(row[-1])
			total_companies += 1

		cur.close()

		to_return = {
			"header":header,
			"results":results,
			"total_demand": total_demand,
			"total_companies": total_companies
		}

		return to_return


	def company_size_query( self, geo_filter=None, seg_filter=None, products=None  ):
		"""Show demand and employee count totals for given inputs"""
		if not self.valid_geo_filter(geo_filter):
			raise Exception("geo_filter {}".format(geo_filter))
		if not self.valid_products(products):
			raise Exception("products {}".format(products))
		if not self.valid_seg_filter(seg_filter):
			raise Exception("seg_filter {}".format(seg_filter))

		# build the geo part of the where query
		geo_query = self.build_geo_filter_where_query(geo_filter=geo_filter)
		seg_query = self.build_seg_filter_where_query(seg_filter=seg_filter)
		extent = self.min_extent( geo_filter )

		cur = self.conn.cursor()

		'''cur.execute("""
			select 
			l.company_size, 
			round(sum(l.employees*r.ratio)) as demand,
			count(*) as companies
			from locations l
			inner join (select sic, sum(ratio) as ratio
				from ratios r 
				where product_id=ANY(%s)
				group by sic) as r on r.sic=l.sic
			inner join geo g on g.id=l.geo_id
			where ({}) and ({})
			group by l.company_size
			order by demand desc ;
		""".format(geo_query, seg_query ), (products, ) )
		'''
		cur.execute("""
			select 
			l.company_size, 
			round(sum(l.employees*r.ratio)) as demand,
			sum(companies) as companies
			from locations_{} l
			inner join (select sic, sum(ratio) as ratio
				from ratios r 
				where product_id=ANY(%s)
				group by sic) as r on r.sic=l.sic
			where ({}) and ({})
			group by l.company_size
			order by demand desc ;
		""".format(extent,geo_query, seg_query ), (products, ) )

		header = ["Company Size","Demand", "Companies"]
		results = []
		total_demand = 0
		total_companies = 0
		for row in cur:
			results.append( row )
			total_demand += int(row[-2])
			total_companies += int(row[-1])

		cur.close()

		to_return = {
			"header":header,
			"results":results,
			"total_demand": total_demand,
			"total_companies": total_companies
		}

		return to_return



	def demographics( self, geo_filter=None, seg_filter=None, products=None  ):
		"""Show company counts totals for by consuming sic"""
		if not self.valid_geo_filter(geo_filter):
			raise Exception("geo_filter {}".format(geo_filter))
		if not self.valid_products(products):
			raise Exception("products {}".format(products))
		if not self.valid_seg_filter(seg_filter):
			raise Exception("seg_filter {}".format(seg_filter))

		# build the geo part of the where query
		geo_query = self.build_geo_filter_where_query(geo_filter=geo_filter)
		seg_query = self.build_seg_filter_where_query(seg_filter=seg_filter)
		extent = self.min_extent( geo_filter )

		cur = self.conn.cursor()
		'''cur.execute("""
				select 
				l.company_size,
				l.sic,
				s.description,
				count(*) as companies
				from locations l
				inner join (select sic, sum(ratio) as ratio
					from ratios r 
					where product_id=ANY(%s)
					group by sic) as r on r.sic=l.sic
				inner join geo g on g.id=l.geo_id
				left join sic s on s.sic=l.sic
				where ({}) and ({})
				group by l.company_size, l.sic, s.description
		""".format( geo_query, seg_query ), (products, ) )
		'''
		cur.execute("""
				select 
				l.company_size,
				l.sic,
				s.description,
				sum(companies) as companies
				from locations_{} l
				inner join (select sic, sum(ratio) as ratio
					from ratios r 
					where product_id=ANY(%s)
					group by sic) as r on r.sic=l.sic
				left join sic s on s.sic=l.sic
				where ({}) and ({})
				group by l.company_size, l.sic, s.description
		""".format( extent,geo_query, seg_query ), (products, ) )
		header = [ "Company Size", "SIC","Description", "Companies"]
		results = []
		total = 0
		for row in cur:
			results.append( row )
			total += int(row[-1])

		cur.close()

		to_return = {
			"header":header,
			"results":results,
			"total": total
		}

		return to_return


	def location_demand( self, duns=None, products=None ):
		"""estimate demand for a location"""
		if not self.valid_duns(duns):
			raise Exception("duns {}".format(duns))
		if not self.valid_products(products):
			raise Exception("products {}".format(products))

		cur = self.conn.cursor()
		cur.execute("""
			select l.duns, round(sum(l.employees*r.ratio)) as demand
			from locations l
			inner join ratios r on r.sic=l.sic
			where 
			l.duns=%s
			and r.product_id=ANY(%s)
			group by l.duns
		""", (duns, products ) )
		header = [ "Duns","Demand"]
		result = cur.fetchone()

		if result:
			to_return = {
				"duns":duns,
				"demand":result[1]
			}
		else:
			to_return = {
				"duns":duns,
				"demand":0
			}

		return to_return


	def total_query( self, geo_filter=None, seg_filter=None, products=None  ):
		"""Show total demand"""
		if not self.valid_geo_filter(geo_filter):
			raise Exception("geo_filter {}".format(geo_filter))
		if not self.valid_products(products):
			raise Exception("products {}".format(products))
		if not self.valid_seg_filter(seg_filter):
			raise Exception("seg_filter {}".format(seg_filter))

		# build the geo part of the where query
		geo_query = self.build_geo_filter_where_query(geo_filter=geo_filter)
		seg_query = self.build_seg_filter_where_query(seg_filter=seg_filter)
		extent = self.min_extent( geo_filter )

		cur = self.conn.cursor()
	
		'''cur.execute("""
			select 
			round(sum(l.employees*r.ratio)) as demand,
			count(*) as companies
			from locations l
			inner join (select sic, sum(ratio) as ratio
				from ratios r 
				where product_id=ANY(%s)
				group by sic) as r on r.sic=l.sic
			inner join geo g on g.id=l.geo_id
			where ({}) and ({})
		""".format(geo_query, seg_query ), (products, ) )
		'''
		cur.execute("""
			select 
			round(sum(l.employees*r.ratio)) as demand,
			sum(companies) as companies
			from locations_{} l
			inner join (select sic, sum(ratio) as ratio
				from ratios r 
				where product_id=ANY(%s)
				group by sic) as r on r.sic=l.sic
			where ({}) and ({})
		""".format(extent,geo_query, seg_query ), (products, ) )

		header = ["Demand","Companies"]
		result = cur.fetchone()
		total_demand = result[0]
		total_companies = result[1]

		if total_demand is None or total_companies is None:
			total_demand = 0
			total_companies = 0

		cur.close()

		to_return = {
			"header":header,
			"total_demand": total_demand,
			"total_companies": total_companies
		}

		return to_return


	def product_list( self, category=None ):
		cur = self.conn.cursor()

		results = []
		if category:
			cur.execute("""
				select * from products
				where category=%s
				order by category, description
				""",(category,))
		else:
			cur.execute("""
				select * from products
				where category is not NULL
				order by category, description
				""")

		for row in cur:
			results.append(row)

		cur.close()

		to_return = {
			"header": ['id','description','type','category', 'extended_description'],
			"results": results,
		}

		return to_return

	def product( self, product_id=None ):
		if not self.valid_products([product_id]):
			raise Exception("products {}".format([product]))

		cur = self.conn.cursor()

		product = {}

		cur.execute("""
			select * from products
			where product_id=%s
			limit 1
			""",(product_id,))

		row = cur.fetchone()
		if row:
			product['product_id'] = row[0]
			product['description'] = row[1]
			product['type'] = row[2]
			product['category'] = row[3]
			product['extended'] = row[4]			

		cur.close()

		return product
