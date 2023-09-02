# 1. Introdução

o ecossistema Hadoop é um conjunto de tecnologias de código aberto projetado para armazenar, processar, analisar e gerenciar grandes volumes de dados de forma distribuída. Ele é amplamente utilizado para lidar com os desafios relacionados ao processamento de dados em escala, como os gerados por empresas, instituições de pesquisa e diversas indústrias.
O componente central do ecossistema Hadoop é o Hadoop Distributed File System (HDFS), um sistema de arquivos distribuído que divide os dados em blocos e os distribui em vários nós de um cluster de servidores. Isso permite o armazenamento e processamento eficiente de grandes conjuntos de dados.

Por sua vez, o Apache Hive é um sistema de data warehouse distribuído e tolerante a falhas que possibilita análises de dados em alta escala e facilita a leitura, escrita e gerenciamento de petabytes de dados armazenados em sistemas de armazenamento distribuído. O Hive se destaca como um componente fundamental no ecossistema Hadoop, permitindo a análise de dados em larga escala e proporcionando uma interface familiar baseada em SQL para a manipulação de conjuntos de dados.

Neste projeto, buscou-se investigar a aplicação prática do Apache Hive junto com a integração com o Hadoop para a análise das informações fornecidas pelos dados de músicas e artistas do Spotify, buscando compreender como esse ecossistema pode ser empregado na extração de insights valiosos a partir de grandes volumes de dados. Ao explorar as funcionalidades do Hive e sua integração com os dados da plataforma de streaming, esperou-se obter uma visão mais profunda das capacidades analíticas que essa combinação pode oferecer.
 
 Na *amostra de dados e definição dos esquemas*, será apresentado os esquemas dos dados que foram utilizados para o desenvolvimento das análises. Em *análise dos dados*, será desenvolvida cinco consultas SQL utilizando-se o Apache Hive para a exploração e análise dos dados anteriormente apresentados e, por fim, em *considerações finais e próximos passos*, será proposto algumas aplicações que poderão ser desenvolvidas com base nessas análises.

# 2. Amostra dos dados e definição dos esquemas

Para o desenvolvimento das análises, foi utilizado o dataset [Spotify 1921-2020](https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks), disponível gratuitamente através do Kaggle. O dataset é constituído por duas tabelas: *artists* e *tracks*, que compreende as músicas e os artistas desde 1921 até 2020.

A tabela *artists* e *tracks* são compostas por 1162095 e 586672 linhas, respectivamente. Uma música pode ter um ou mais artistas atribuídos a ela. Ambas tabelas foram armazenadas no Apache Hive no formato textfile, carregando os dados diretamente dos arquivos CSV para suas respectivas tabelas. O DLL para o carregamento desses dados pode ser conferido [aqui](https://github.com/geazi-anc/spotify/blob/main/ddl/load-data.sql).

## Tabela Artists

| Coluna     | Tipo           |
|------------|----------------|
| id         | STRING         |
| followers  | INT            |
| genres     | ARRAY<STRING>  |
| name       | STRING         |
| popularity | INT            |

O DDL referente a criação a tabela artists pode ser conferido [aqui](https://github.com/geazi-anc/spotify/blob/main/ddl/create_artists-table.sql).

## Tabela tracks

| Coluna        | Tipo           |
|---------------|----------------|
| id            | STRING         |
| name          | STRING         |
| popularity    | INT            |
| release_date  | DATE           |
| duration_ms   | BIGINT         |
| explicit      | INT            |
| id_artists    | ARRAY<STRING>  |

O DDL referente a criação da tabela tracks pode ser conferido [aqui](https://github.com/geazi-anc/spotify/blob/main/ddl/create-tracks-table.sql).

# 3. Análise dos dados

Para a consolidação dos conhecimentos adquiridos do ecossistema hadoop e do Apache Hive, serão desenvolvidos cinco consultas SQL para a exploração e análise dos dados anteriormente apresentados.

## 3.1. Artistas com músicas mais populares

Com essa análise, buscou-se analisar os três principais artistas que tiveram a maior somatória de popularidade de suas músicas e que ano em que esse record ocorreu. O DML dessa consulta pode ser conferido [aqui](https://github.com/geazi-anc/spotify/blob/main/dml/01.sql).

|     artist_name      |  year  |  total_tracks_popularity  |
|----------------------|--------|--------------------------|
| Die drei ???         |  1980  |  16710                   |
| Die drei ???         |  1979  |  15918                   |
| TKKG Retro-Archiv    |  1982  |  12257                   |

## 3.2. Média da duração das músicas

Essa análise teve como objetivo analisar os dez principais artistas cuja somatória da duração de suas músicas fosse no máximo 50% maior do que a média geral da duração das músicas de todos os artistas. O DML dessa consulta pode ser conferido [aqui](https://github.com/geazi-anc/spotify/blob/main/dml/02.sql).

|         artist_name         |  track_duration_s  |
|---------------------------- |--------------------|
| Elan                        |  339.61            |
|  'classic rock'             |  339.61            |
|  'desi pop'                 |  339.61            |
|  'art rock'                 |  339.6             |
|  'turkish jazz']"           |  339.6             |
|  'turkish folk'             |  339.6             |
|  'dance rock'               |  339.6             |
|  'czech rock']"             |  339.6             |
|  'compositional ambient'    |  339.6             |
|  'classic italian pop'      |  339.6             |

## 3.3. Artista com mais seguidores

Dessa vez, analisou-se o artista com a maior quantidade de seguidores e quais de suas músicas é a mais popular. O DML dessa consulta pode ser conferido [aqui](https://github.com/geazi-anc/spotify/blob/main/dml/03.sql).

|  artist_name   |  total_followers  |  most_popular_track  |
|--------------- |-------------------|----------------------|
|  'uk pop']"   |  78900234         |  Afterglow           |

## 3.4. Décadas com mais lançamento de músicas

Com essa análise, procurou determinar as décadas que tiveram a maior quantidade de lançamento de músicas, assim como as décadas que tiveram mais músicas populares. O DML dessa consulta pode ser conferido [aqui](https://github.com/geazi-anc/spotify/blob/main/dml/04.sql).

|  decade  |  total_released_tracks  |  total_popularity  |
|--------- |------------------------ |------------------- |
|  2010    |  98737                  |  3893398           |
|  1990    |  81002                  |  2407841           |
|  2000    |  72155                  |  2654656           |
|  1980    |  55925                  |  1425391           |
|  1970    |  38889                  |  939957            |
|  1960    |  28522                  |  518393            |
|  2020    |  19635                  |  828177            |
|  1950    |  18036                  |  160933            |
|  1940    |  8826                   |  10712             |
|  1930    |  5517                   |  6323              |
|  1920    |  2942                   |  215               |
|  1900    |  1                      |  19                |

## 3.5. Música mais popular da década

Por fim, essa análise identifica qual foi a música mais popular da década, desde 1921 até 2020. O DML dessa consulta pode ser conferido [aqui](https://github.com/geazi-anc/spotify/blob/main/dml/05.sql).

|  decade  |             most_popular_track              |  track_popularity  |
|--------- |-------------------------------------------- |------------------- |
|  2020S   |  drivers license                          |  99                |
|  2010S   |  Streets                                  |  94                |
|  2000S   |  Cupid's Chokehold / Breakfast in America |  87                |
|  1990S   |  Losing My Religion                       |  83                |
|  1980S   |  Take on Me                               |  86                |
|  1970S   |  Dreams - 2004 Remaster                   |  86                |
|  1960S   |  Here Comes The Sun - Remastered 2009     |  83                |
|  1950S   |  Johnny B. Goode                          |  77                |
|  1940S   |  Dream                                    |  56                |
|  1930S   |  Moonlight Serenade - 2005 Remastered Ver.|  53                |
|  1920S   |  Sugar                                    |  17                |
|  1900S   |  Maldita sea la primera vez               |  19                |

# 4. Considerações finais e próximos passos

A análise de dados realizada utilizando o Apache Hive em um conjunto de dados provenientes do Spotify demonstrou a capacidade poderosa dessa ferramenta no processamento e exploração de informações em larga escala. A combinação das tabelas "tracks" e "artists" proporcionou uma visão abrangente sobre as tendências musicais, popularidade das faixas e artistas, bem como padrões de lançamento e duração de músicas.

Através da execução de consultas complexas e janelas de análise no Hive, foi possível identificar insights valiosos que podem orientar decisões estratégicas no cenário da indústria musical. Por exemplo, ao analisar a popularidade das faixas por década, pudemos observar como os gostos musicais evoluíram ao longo do tempo, destacando os artistas e gêneros mais influentes em cada período.

Uma das aplicações promissoras dos dados coletados, como uma proposta de solução futura, é a construção de um modelo de machine learning para prever a popularidade futura de novas faixas com base nas características dos artistas, gêneros e outras variáveis relevantes. Utilizando algoritmos de regressão ou classificação, é possível criar um modelo que ajude as gravadoras e plataformas de streaming a direcionar seus esforços de promoção e recomendação de músicas, maximizando o impacto e alcance das novas faixas.
