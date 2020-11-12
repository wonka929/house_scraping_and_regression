#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
#from tqdm import tqdm_notebook as tqdm
from tqdm.notebook import tqdm as tqdm
import csv
from joblib import Parallel, delayed
import multiprocessing


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
    for ultag in data.find_all('ul', {'class': 'breadcrumb-list breadcrumb-list_list breadcrumb-list__related'}):
        for litag in ultag.find_all('li'):
            for i in range(len(litag.text.split(','))):
                areas.append(litag.text.split(',')[i])
    areas = [x.strip() for x in areas]
    urls = []
    
    for area in areas:
        url = website + '/' + area.replace(' ','-').lower()
        urls.append(url)
    
    return areas, urls

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

#def scrape_link(website):
#    data = connect(website)
#    nomi = []
#    valori = []
#    temp = []
#    for link in (data.find_all('dd', {'class': 'im-features__value'})[1], data.find_all('dd', {'class': 'im-features__value'})[9], data.find_all('dd', {'class': 'im-features__value'})[12]):
#        temp.append(link)
#        try:
#            temp = str(temp).replace('</dd>', '')
#            temp = str(temp).replace('<dt', '</dd><dt')
#            temp = BeautifulSoup(temp, "html.parser")
#            for elem in temp.find_all('dd'):
#                valori.append(elem.string.strip())
#        except:
#            #print('errore')
#            pass
#        temp = []
#
#    valori[-1] = (data.find_all('dd', {'class': 'im-features__value'})[0].find_all('span')[0].string)
#
#    for link in data.find_all('dt'):
#        if link.string == None:
#            pass
#        else:
#            nomi.append(link.string.strip())
#
#    while len(valori)<len(nomi):
#        valori.append(0)
#
#    while len(nomi)<len(valori):
#        nomi.append('Prestazione energetica del fabbricato')
#
#    count = 0
#    nomi.append('Arredato S/N')
#    nomi.remove('altre caratteristiche')
#    for elem in data.find_all('dd', {'class': 'im-features__value'})[8].find_all('span'):
#        if elem.string.strip() == "Arredato" or elem.string == "Non arredato" or elem.string == "Parzialmente arredato":
#            valori.append(elem.string.strip())
#            count = 1            
#        else:
#            continue
#    if count == 0:
#        valori.append('0')        
#
#    valori = remove_duplicates(valori)
#    nomi = remove_duplicates(nomi)
#    
#    return nomi, valori

def scrape_link(website):
    data = connect(website)
    info = data.find_all('dl', {'class': 'im-features__list'})
    comp_info = pd.DataFrame()
    cleaned_id_text = []
    cleaned_id__attrb_text = []
    for n in range(len(info)):
        for i in info[n].find_all('dt'):
            cleaned_id_text.append(i.text)
        for i in info[n].find_all('dd'):
            cleaned_id__attrb_text.append(i.text)

    comp_info['Id'] = cleaned_id_text
    comp_info['Attribute'] = cleaned_id__attrb_text
    comp_info
    feature = []
    for item in comp_info['Attribute']:
        try:
            feature.append(clear_df(item))
        except:
            feature.append(ultra_clear_df(item))

    comp_info['Attribute'] = feature
    #comp_info.set_index('Id', drop=True, inplace=True)
    #comp_info.index.name = None
    #comp_info = comp_info.T.reset_index().drop(columns='index')
    return comp_info['Id'].values, comp_info['Attribute'].values
    

def remove_duplicates(x):
    return list(dict.fromkeys(x))


def clear_df(the_list):
    the_list = (the_list.split('\n')[1].split('  '))
    the_list = [value for value in the_list if value != ''][0]
    return the_list

def ultra_clear_df(the_list):
    the_list = (the_list.split('\n\n')[1].split('  '))
    the_list = [value for value in the_list if value != ''][0]
    the_list = (the_list.split('\n')[0])
    return the_list


# In[3]:


## Link for city main website
## get areas inside the city (districts)

website = "https://www.immobiliare.it/affitto-case/torino"
areas, districts = get_areas(website)
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

df_scrape = pd.DataFrame()
to_be_dropped = []
counter = 0
for link in tqdm(list(announces_links)):
    counter=counter+1
    try:
        nomi, valori = scrape_link(link)
        df_temporaneo = pd.DataFrame(columns=nomi)
        df_temporaneo.loc[len(df_temporaneo), :] = valori[0:len(nomi)]
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


for index in to_be_dropped:
    del location[index-1]


# In[10]:


for index in to_be_dropped:
    del announces_links[index-1]


# In[11]:


print(df_scrape.shape)
df_scrape['zona'] = location
df_scrape['links'] = announces_links
df_scrape.columns = map(str.lower, df_scrape.columns)
df_scrape.to_csv('dataset.csv', sep=";")


# In[12]:


df_scrape = pd.read_csv('dataset.csv', sep=';')


# In[13]:


df_scrape = df_scrape[['contratto', 'zona', 'tipologia', 'superficie', 'locali', 'piano', 'tipo proprietà', 'prezzo', 'spese condominio', 'spese aggiuntive', 'anno di costruzione', 'stato', 'riscaldamento', 'climatizzazione', 'classe energetica', 'posti auto', 'links']]


# In[14]:


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
    
    for tipo in df['tipologia']:
        try:
            tipologia.append(tipo)
        except:
            tipologia.append(None)
    
    for superficie in df['superficie']:
        try:
            if "m" in superficie:
                s = superficie.replace(" m²", "")
                surface.append(s)
        except:
            surface.append(None)
    
    for locali in df['locali']:
        try:
            rooms.append(locali[0:1])
        except:
            rooms.append(None)
    
    for prezzo in df['prezzo']:
        try:
            price.append(prezzo.replace("Affitto ", "").replace("€ ", "").replace("/mese", "").replace(".",""))
        except:
            price.append(None)
            
    for contratto in df['contratto']:
        try:
            contract.append(contratto.replace("\n ",""))
        except:
            contract.append(None)
    
    for piano in df['piano']:
        try:
            floor.append(piano.split(' ')[0])
        except:
            floor.append(None)
    
    for tipologia in df['tipo proprietà']:
        try:
            pr_type.append(tipologia.split(',')[0])
        except:
            pr_type.append(None)
            
    for condo in df['spese condominio']:
        try:
            if "mese" in condo:
                condominio.append(condo.replace("€ ","").replace("/mese",""))
            else:
                condominio.append(None)
        except:
            condominio.append(None)
        
    for ii in df['spese aggiuntive']:
        try:
            if "anno" in ii:
                mese = int(int(ii.replace("€ ","").replace("/anno","").replace(".",""))/12)
                heating.append(mese)
            else:
                heating.append(None)
        except:
            heating.append(None)
   
    for anno_costruzione in df['anno di costruzione']:
        try:
            built_in.append(anno_costruzione)
        except:
            built_in.append(None)
    
    for stato in df['stato']:
        try:
            stat = stato.replace(" ","").lower()
            state.append(stat)
        except:
            state.append(None)
    
    for tipo_riscaldamento in df['riscaldamento']:
        try:
            if 'Centralizzato' in tipo_riscaldamento:
                riscaldamento.append('centralizzato')
            elif 'Autonomo' in tipo_riscaldamento:
                riscaldamento.append('autonomo')
        except:
            riscaldamento.append(None)
    
    for clima in df['climatizzazione']:
        try:
            cooling.append(clima.lower().split(',')[0])
        except:
            cooling.append('None')
    
    for classe in df['classe energetica']:
        try:
            energy_class.append(classe[0:2])#.replace("\n ",""))
        except:
            energy_class.append(None)
            
    #for SN in df['Arredato S/N']:
    #    try:
    #        arredato.append(SN)
    #    except:
    #        arredato.append(None)
    #        
    
    final_df = pd.DataFrame(columns=['contratto', 'zona', 'tipologia', 'superficie', 'locali', 'piano', 'tipo proprietà', 'prezzo', 'spese condominio', 'spese aggiuntive', 'anno di costruzione', 'stato', 'riscaldamento', 'climatizzazione', 'certificazione energetica', 'posti auto'])#, 'Arredato S/N'])
    final_df['contratto'] = contract
    final_df['tipologia'] = tipologia
    final_df['superficie'] = surface
    final_df['locali'] = rooms
    final_df['piano'] = floor
    final_df['tipo proprietà'] = pr_type
    final_df['prezzo'] = price
    final_df['spese condominio'] = condominio
    final_df['spese riscaldamento'] = heating
    final_df['anno di costruzione'] = built_in
    final_df['stato'] = state
    final_df['riscaldamento'] = riscaldamento
    final_df['climatizzatore'] = cooling
    final_df['classe energetica'] = energy_class
    final_df['zona'] = df['zona'].values
    #inal_df['Arredato S/N'] = arredato
    final_df['link annuncio'] = announces_links
    
    return final_df


# In[15]:


final = cleanup(df_scrape)
final.to_csv('dataset_da_allenamento.csv', sep=";")


# In[ ]:




