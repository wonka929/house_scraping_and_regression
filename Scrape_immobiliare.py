#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm as tqdm
import csv


# In[2]:


def get_pages(main):
    try:
        soup = connect(main)
        n_pages = [_.get_text(strip=True) for _ in soup.find('ul', {'class': 'pagination pagination__number'}).find_all('li')]
        #max = soup.find_all("span", class_="pagination__number")
        last_page = int(n_pages[-1])
        pages = [main]
        
        for n in range(2,last_page+1):    
            page_num = "/?pag={}".format(n)
            pages.append(main + page_num)
    except:
        pages = [main]
        
    return pages

def connect(web_addr):
    resp = requests.get(web_addr)
    return BeautifulSoup(resp.content, "html.parser")
    

def get_areas(website):
    data = connect(website)
    areas = []
    for ultag in data.find_all('ul', {'class': 'breadcrumb-list breadcrumb-list_list thebigonelist--mouse'}):
        for litag in ultag.find_all('li'):
            for i in range(len(litag.text.split(','))):
                areas.append(litag.text.split(',')[i])
    areas = [x.strip() for x in areas]
    urls = []
    
    for area in areas:
        url = website + '/' + area.replace(' ','-').lower()
        urls.append(url)
    
    return urls

def get_apartment_links(website):
    data = connect(website)
    links = []
    for link in data.find_all('ul', {'class': 'annunci-list'}):
        for litag in link.find_all('li'):
            try:
                #return litag.a.get('href')
                links.append(litag.a.get('href'))
            except:
                continue
    return links

def scrape_link(website):
    data = connect(website)
    nomi = []
    valori = []
    for link in data.find_all('dd', {'class': 'col-xs-12'}):
        try:
            valore_affitto = link.find("span")
            if valore_affitto.string != None:
                valori.append(valore_affitto.string)
        except:
            pass
        
        if link.string == None:
            pass
        else:
            valori.append(link.string)
    
        
    for link in data.find_all('dt'):
        if link.string == None:
            pass
        else:
            nomi.append(link.string)
    
    valori = remove_duplicates(valori)
    nomi = remove_duplicates(nomi)
    
    while len(valori)<len(nomi):
        valori.append(0)
        
    while len(nomi)<len(valori):
        nomi.append('Prestazione energetica del fabbricato')
    
    count = 0
    nomi.append('Arredato S/N')
    for elem in data.find_all('span', {'class': 'label label-gray'}):
        if elem.string == "Arredato" or elem.string == "Non arredato" or elem.string == "Parzialmente arredato":
            valori.append(elem.string)
            count = 1            
        else:
            continue
    if count == 0:
        valori.append('0')
    
    return nomi, valori

def remove_duplicates(x):
    return list(dict.fromkeys(x))


# In[3]:


## Link for city main website
## get areas inside the city (districts)

website = "https://www.immobiliare.it/affitto-case/torino"
districts = get_areas(website)
print("Those are district's links \n")
print(districts)


# ## Scrape cycle initialization

# In[4]:


indirizzo = []
location = []

for url in tqdm(districts):
    pages = get_pages(url)
    for page in pages:
        add = get_apartment_links(page)
        indirizzo.append(add)
        for num in range(0,len(add)):
            location.append(url.rsplit('/', 1)[-1])
        
announces_links = [item for valore in indirizzo for item in valore]


# In[5]:


## Check that what you scraped has a meaning...and save it

print("The numerosity of announces:\n")
print(len(announces_links))
with open('announces_list.csv', 'w') as myfile:
    wr = csv.writer(myfile)
    wr.writerow(announces_links)


# ## Proper dataset creation by scraping every announce

# In[6]:


## DATAFRAME ORIZZONTALE__USARE

variabili = ['Riferimento e Data annuncio', 'Contratto', 'Tipologia', 'Superficie', 'Locali', 'Piano', 'Disponibilità', 'Tipo proprietà', 'Prezzo', 'Spese condominio', 'Spese riscaldamento', 'Informazioni catastali', 'Anno di costruzione', 'Stato', 'Riscaldamento', 'Climatizzatore', 'Classe energetica', 'Prestazione energetica del fabbricato', 'Arredato S/N']
df_scrape = pd.DataFrame(columns=variabili)
#df_scrape.set_index(0, inplace=True, drop=True)
#print(df_scrape)
to_be_dropped = []
counter = 0
for link in tqdm(list(announces_links)):
    #print(link)
    counter=counter+1
    try:
        nomi, valori = scrape_link(link)
        df_temporaneo = pd.DataFrame(columns=nomi)
        #df_temporaneo['Nomi'] = nomi
        #df_temporaneo.set_index('Nomi',inplace=True,drop=True)
        #print(len(nomi),len(valori))
        #while len(valori)<len(nomi):
        #    valori.append(0)
        #print(len(valori[0:len(nomi)]))
        df_temporaneo.loc[len(df_temporaneo), :] = valori[0:len(nomi)]
        #print(df_temporaneo)
        df_scrape = df_scrape.append(df_temporaneo, sort=False)
    except Exception as e:
        print(e)
        to_be_dropped.append(counter)
        print(to_be_dropped)
        continue


# In[7]:


#for item in to_be_dropped:
pd.DataFrame(location).to_csv('location.csv', sep=';')
pd.DataFrame(to_be_dropped).to_csv('to_be_dropped.csv', sep=';')


# In[8]:


to_be_dropped.sort(reverse=True)


# In[9]:


to_be_dropped


# In[10]:


for index in to_be_dropped:
    del location[index-1]


# In[11]:


for index in to_be_dropped:
    del announces_links[index-1]


# In[12]:


len(location)


# In[13]:


print(df_scrape.shape)
df_scrape['Zona'] = location
df_scrape.to_csv('dataset.csv', sep=";")


# In[14]:


df_scrape = df_scrape[['Contratto', 'Zona', 'Tipologia', 'Superficie', 'Locali', 'Piano', 'Tipo proprietà', 'Prezzo', 'Spese condominio', 'Spese riscaldamento','Anno di costruzione', 'Stato', 'Riscaldamento', 'Climatizzatore', 'Classe energetica', 'Arredato S/N']]


# In[23]:


def cleanup(df):
    price = []
    rooms = []
    surface = []
    bathrooms = []
    floor = []
    contract = []
    tipo = []
    condominio = []
    heating = []
    built_in = []
    state = []
    riscaldamento = []
    cooling = []
    energy_class = []
    tipologia = []
    pr_type = []
    arredato = []
    
    for tipo in df['Tipologia']:
        try:
            tipologia.append(tipo)
        except:
            tipologia.append(None)
    
    for superficie in df['Superficie']:
        try:
            if "m" in superficie:
                s = superficie.replace(" m²", "")
                surface.append(s)
        except:
            surface.append(None)
    
    for locali in df['Locali']:
        try:
            rooms.append(locali[0:1])
        except:
            rooms.append(None)
    
    for prezzo in df['Prezzo']:
        try:
            price.append(prezzo.replace("Affitto ", "").replace("€ ", "").replace(" al mese", "").replace(".",""))
        except:
            price.append(None)
            
    for contratto in df['Contratto']:
        try:
            contract.append(contratto.replace("\n ",""))
        except:
            contract.append(None)
    
    for piano in df['Piano']:
        try:
            floor.append(piano.split(' ')[0])
        except:
            floor.append(None)
    
    for tipologia in df['Tipo proprietà']:
        try:
            pr_type.append(tipologia.split(',')[0])
        except:
            pr_type.append(None)
            
    for condo in df['Spese condominio']:
        try:
            if "mese" in condo:
                condominio.append(condo.replace("€ ","").replace("/mese",""))
            else:
                condominio.append(None)
        except:
            condominio.append(None)
        
    for ii in df['Spese riscaldamento']:
        try:
            if "anno" in ii:
                mese = int(int(ii.replace("€ ","").replace("/anno","").replace(".",""))/12)
                heating.append(mese)
            else:
                heating.append(None)
        except:
            heating.append(None)
    
    for anno_costruzione in df['Anno di costruzione']:
        try:
            built_in.append(anno_costruzione)
        except:
            built_in.append(None)
    
    for stato in df['Stato']:
        try:
            stat = stato.replace(" ","").lower()
            state.append(stat)
        except:
            state.append(None)
    
    for tipo_riscaldamento in df['Riscaldamento']:
        try:
            riscaldamento.append(tipo_riscaldamento.lower().split(',')[0])
        except:
            riscaldamento.append(None)
    
    for clima in df['Climatizzatore']:
        try:
            cooling.append(clima.lower().split(',')[0])
        except:
            cooling.append('None')
    
    for classe in df['Classe energetica']:
        try:
            energy_class.append(classe.replace("\n ",""))
        except:
            energy_class.append(None)
            
    for SN in df['Arredato S/N']:
        try:
            arredato.append(SN)
        except:
            arredato.append(None)
            
    
    final_df = pd.DataFrame(columns=['Contratto', 'Zona', 'Tipologia', 'Superficie', 'Locali', 'Piano', 'Tipo proprietà', 'Prezzo', 'Spese condominio', 'Spese riscaldamento','Anno di costruzione', 'Stato', 'Riscaldamento', 'Climatizzatore', 'Classe energetica', 'Arredato S/N'])
    final_df['Contratto'] = contract
    final_df['Tipologia'] = tipologia
    final_df['Superficie'] = surface
    final_df['Locali'] = rooms
    final_df['Piano'] = floor
    final_df['Tipo proprietà'] = pr_type
    final_df['Prezzo'] = price
    final_df['Spese condominio'] = condominio
    final_df['Spese riscaldamento'] = heating
    final_df['Anno di costruzione'] = built_in
    final_df['Stato'] = state
    final_df['Riscaldamento'] = riscaldamento
    final_df['Climatizzatore'] = cooling
    final_df['Classe energetica'] = energy_class
    final_df['Zona'] = df['Zona'].values
    final_df['Arredato S/N'] = arredato
    final_df['Link annuncio'] = announces_links
    
    return final_df


# In[24]:


final = cleanup(df_scrape)
final.to_csv('dataset_da_allenamento.csv', sep=";")


# In[ ]:




