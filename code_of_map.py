from telethon import TelegramClient
import time
import branca.colormap as cmp
from datetime import date, timedelta
from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsSyntaxParser,
    NewsNERTagger,
    Doc)
import re
import folium
import pandas as pd
import geopandas as gpd
from geopandas.tools import geocode
import csv
import geopy.distance
from shapely.geometry import Point
from branca.element import Template, MacroElement


# данные для телеграмм бота
api_id = 'your api_id'
api_hash = 'your api_hash'
client = TelegramClient('anon', api_id, api_hash, system_version="4.16.30-vxCUSTOM")

date_of_post=date.fromisoformat('2020-01-01')

yesterday = date.today() - timedelta(days=1)
yesterday.strftime('%m%d%y')
print(yesterday)

list2=[]
news = []
async def main():
    n=0
    
    # You can print the message history of any chat:
    async for message in client.iter_messages('@papagaz', reverse = True, offset_date = yesterday):
        mess = message.sender.username, message.text, message.date
        m = str(mess[1]) + str(message.date) #+ ';' +'\n'
        news.append(m)
    print(news)
with client:
    client.loop.run_until_complete(main())
# 
# ---------------------------------------------------------------------------------------------------------------
#
def main():
#  разпор полученных сообщений
    segmenter = Segmenter()

    emb = NewsEmbedding()
    morph_tagger = NewsMorphTagger(emb)
    syntax_parser = NewsSyntaxParser(emb)

#     #импортируем данные
    words = r"your path...words_true.txt".replace('\\','/')
    w1 = open(words, 'r', encoding="utf-8", errors='ignore')
    wtrue = w1.read().split(',')
    news_need = []
    n=0
    #сопоставляем данные со словарями
    for i in news:
        new = str(i)
        for s in wtrue:
            if new.find(str(s).lower()) != -1:
                # print("True")
                n=n+1
                news_need.append(new)
    
    print('done')
    
    #NLP анализ 
    sentt = []
    if len(news_need)>0:
        for sen in news_need[0].split('\n\n'):
            if re.findall(r"\xa0", sen):
                sen = sen.replace("\xa0", " ")
            sentt.append(sen)

        sen = []
        loc = []
        loc1 = []
        loc2 = []

        print('done2')
        for i in news_need:
            print('----------------------')
            doc = Doc(i)
            doc.segment(segmenter)
            doc.tag_morph(morph_tagger)
            doc.parse_syntax(syntax_parser)
            
            ner_tagger = NewsNERTagger(emb)
            doc.tag_ner(ner_tagger)
            
            morph_vocab = MorphVocab()
            for span in doc.spans:
                span.normalize(morph_vocab)

            temp1=[] 
            for j in doc.spans[:]:
                if j.type =='LOC':
                    temp1.append(j.normal)
                    
            if len(temp1)>1:
                sen.append(i)

                loc1.append(temp1[0])
                loc2.append(temp1[1])
        
            elif len(temp1)==1:
                sen.append(i)
                loc1.append(temp1[0])
                loc2.append(temp1[0])
                
        print('done3')

        # геокодирование
        zipped_loc = zip(loc1,loc2, sen)
        zipped_list_loc= list(zipped_loc)
        df_loc = pd.DataFrame(zipped_list_loc, columns=["11", "22",'3'])
        lon_1 = []
        n='Nan'
        lon_2 = []
        news1=[]
        news2=[]
        for index, row in df_loc.iterrows():
            dataframe = geocode(row[0] , provider="nominatim" , user_agent = 'my_request') 
            if str(dataframe.iloc[0,1]) == 'None':
                lon_1.append(n)
                news1.append( row[2])
                pass
            
            else:
                point = dataframe.geometry.iloc[0] 
                lon_1.append (str(point.y) + ',' + str(point.x))
                news1.append( row[2])

        for index, row in df_loc.iterrows():
            dataframe = geocode(row[1] , provider="nominatim" , user_agent = 'my_request') 
            if str(dataframe.iloc[0,1]) == 'None':
                lon_2.append(n)
                news2.append( row[2])
                pass
                    
            else:
                point = dataframe.geometry.iloc[0] 
                lon_2.append (str(point.y) + ',' +str(point.x))
                news2.append( row[2])
        
        print(loc1, lon_1, loc2, lon_2)
        loc11 = list(zip(loc1, lon_1))
        print(loc11)
        loc22 = list(zip(loc2, lon_2))
        print(loc22)
        dict = {'news1':news1, 'name1': loc1, 'lon1': lon_1, 'news2':news2, 'name2': loc2, 'lon2': lon_2} 
        df_loc = pd.DataFrame(dict)
        df_loc.to_csv (r'your path... /temp_lonlat2.csv',  'a', index= False )
        print('done3-------------')

    else:
        print('no new events found')

    # пространственный анализ
    df = pd.read_csv(r'your path... /temp_lonlat2.csv')
    neew=[]
    lat=[]
    lon=[]
    st=[]

    for index, row in df.iterrows():
        if row[2] !='Nan' and row[5] !='Nan':
            new1=row[0]
            new2=row[3]
            n1 = row[2]
            n2 =row[5]
            arr = n1.split(',')
            arr2 = n2.split(',')
        
            dis = geopy.distance.geodesic(arr, arr2).km
            
            if dis >2000:
                lon.append(arr2[0])
                lat.append(arr2[1])
                neew.append(row[0])
                sta=1
                st.append(sta)
            else:
                lon.append(arr[0])
                lat.append(arr[1])
                neew.append(row[0])
                sta=0
                st.append(sta)
     
    data1 = gpd.datasets.get_path('naturalearth_lowres')
    gdf = gpd.read_file(data1)
    zipped_values = zip(lat, lon, neew, st)
    zipped_list = list(zipped_values)

    df = pd.DataFrame(zipped_list, columns=["latitude", "longitude", "new", 'status'])

    gdf_points = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy( df.latitude,df.longitude)).set_crs("EPSG:4326")
    gdf_points = gdf_points.to_crs(gdf.crs)

    # визуализация
    def color_producer(charec):
        if charec == '1':
            return 'orange'
        elif charec == '0':
            return 'green'
        else:
            return 'yellow'

    loc = "События в нефтрегазовой сфере с 2020 года"
    title_html = '''
                <h3 align="center" style="font-size:16px"><b>{}</b></h3>
                '''.format(loc)

    template = """

    <body>
    <div id='maplegend' class='maplegend' 
        style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.8);
        border-radius:6px; padding: 10px; font-size:14px; right: 50px; bottom: 50px;  width: 200px; height: 160px'>
        
    <div class='legend-title'>Надежность результата привязки</div>
    <div class='legend-scale'>
    <ul class='legend-labels'>
    <li><span class="circle"'></span>надежный</li>
    <li><span class="circle1"'></span>ненадежный</li>
        

    </ul>
    </div>
    </div>
    
    </body>
    </html>

    <style type='text/css'>
    .maplegend .legend-title {
        text-align: left;
        margin-bottom: 5px;
        font-weight: bold;
        font-size: 90%;
        }

        .circle {
        border-radius: 500%;
        width: 20px;
        height: 20px;
        background: green;
    }
    .circle1 {
        border-radius: 500%;
        width: 20px;
        height: 20px;
        background: orange;
    }
    .maplegend .legend-scale ul {
        margin: 10;
        margin-bottom: 5px;
        padding: 10;
        float: left;
        list-style: none;
        }
    .maplegend .legend-scale ul li {
        font-size: 100%;
        list-style: none;
        margin-left: 10;
        line-height: 28px;
        margin-bottom: 12px;
        
        }
    .maplegend ul.legend-labels li span {
        display: block;
        float: left;
        height: 20px;
        width: 40px;
        margin-right: 5px;
        margin-left: 10;
        
        border: 1px solid #999;
        }
    .maplegend .legend-source {
        font-size: 80%;
        color: #777;
        clear: both;
        }
    .maplegend a {
        color: #777;
        }
    p.dline {
                        line-height: 1.5;
                    }
                    P {
                        line-height: 0.9em;
                    }
    </style>
    """

    data_merged = gpd.sjoin( gdf_points, gdf, how="inner", predicate='intersects')
    data_m = data_merged.groupby(['name'], as_index=False,)['new'].count()

    data_m.columns = ['name', 'count_new']  
    
    data_m.reset_index(inplace= True )
    g = data_m.merge(gdf, how='left', on='name')
    j = gdf.to_json()
 
    data_m2 = data_merged.groupby('name').agg('count')
    data_m2.reset_index(inplace= True )
    g2 = data_m2.merge(gdf, how='left', on='name')

    mapit = folium.Map( location=[0, 0], zoom_start=2, max_zoom=6, min_zoom=2 )

    step = cmp.StepColormap(
    ['#F2D7D5', '#D98880', '#CD6155','#C0392B'],
    vmin=1, vmax=15,
    index=[3, 5, 9, 15],  
    caption='количество событий'    
    )

    map_dict = g.set_index('name')['count_new'].to_dict()
    
    def pol_color(value):
        if int(value)<2:
            return '#F2D7D5'
        elif int(value)<5 and int(value)>=2:
            return '#D98880'
        elif int(value)<9 and int(value)>=5:
            return'#CD6155'
        elif int(value)>=9:
            return '#C0392B' 
        else:
            return '#C0392B'
        
    style_function=lambda feature: {
                    'fillOpacity': 0.5,
                    'weight': 0.5,
                    'color': pol_color( feature['properties']['name'])}
                    
    for _, r in g2.iterrows():
        sim_geo = gpd.GeoSeries(r['geometry_y']).simplify(tolerance=0.001)
        ne = pd.Series(r['new'])
        ht = gpd.GeoDataFrame({'name': ne, 'geometry':sim_geo})
        geo_j = ht.to_json()
                
        geo_j = folium.GeoJson(data=geo_j,
                            style_function=  style_function)
        geo_j.add_to(mapit)
    step.add_to(mapit) 

    for lat , lon, list_loc,charec in zip(  df.latitude , df.longitude, df.new, df.status ): folium.CircleMarker( location=[ lon, lat ], popup= folium.Popup(list_loc, max_width=200, max_height=300), fill_color=color_producer(str(charec)), color=color_producer(str(charec)), radius=5 ).add_to( mapit )
    mapit.get_root().html.add_child(folium.Element(title_html))
    mapit.get_root().html.add_child(folium.Element(template))
    mapit.show_in_browser()
    mapit.save("map.html")

if __name__=="__main__":
    main()

