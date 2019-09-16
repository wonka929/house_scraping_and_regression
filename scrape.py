import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

website = "https://www.immobiliare.it/affitto-case/torino"

def connect(web_addr):
	resp = requests.get(web_addr)
	return BeautifulSoup(resp.content, "html.parser")
	
def get_pages(main):
	soup = connect(main)
	max = soup.find_all("span", class_="pagination__label")
	last_page = int(max[-1].contents[0])
	pages = [main]
	
	for n in range(2,last_page):	
		page_num = "/?pag={}".format(n)
		pages.append(main + page_num)
		
	return pages

def create_df(offers):
	price = []
	rooms = []
	surface = []
	bathrooms = []
	floor = []
	
	for offer in offers:
		l = list(offer.stripped_strings)
		
		if "€" in l[0]:
			stripped = l[0].replace("€ ", "").replace(".","")
			price.append(stripped)
		else:
			price.append(None)
			
		if "locali" in l:
			r = l.index("locali")-1
			rooms.append(l[r])
		else:
			rooms.append(None)
			
		if "m" in l:
			s = l.index("m")-1
			surface.append(l[s])
		else:
			surface.append(None)
			
		if "bagni" in l:
			b = l.index("bagni")-1
			bathrooms.append(l[b])
		else:
			bathrooms.append(None)
			
		if "piano" in l:
			fl = l.index("piano")-1
			floor.append(l[fl])
		else:
			floor.append(None)
			
	return pd.DataFrame.from_dict({"Price": price, "Rooms": rooms, "Surface": surface, "Bathrooms": bathrooms, "Floor": floor})
	
def collect():
	pages = get_pages(website)
	df = pd.DataFrame(columns=["Price", "Rooms", "Surface", "Bathrooms", "Floor"])
	
	for page in tqdm(pages):
		soup = connect(page)
		offers = soup.find_all("ul", class_="listing-features list-piped")
		data = create_df(offers)
		df = df.append(data, ignore_index=True)
				
		
	return df	
		
data = collect()

data.to_csv('output.csv', sep=',', decimal='.')
print(data)
