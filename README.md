MovieCrawler Web Application
=============================
[ ![Codeship Status for ChenLiZhan/MovieCrawler](https://codeship.com/projects/b23a7d90-4a4e-0132-e0ce-3a47b25aadbc/status)](https://codeship.com/projects/46254)
## Our Heroku web service
Take the [link](https://movie-crawler.herokuapp.com/) to use our service.

## Description
You can use our web application to check the informations of movies from famous @movie in taiwan. For example, you can quickly take a look at the informations of latest, second-round movies. Furthermore, we also provide you to get the top 10 famous moives and DVD rank.

## API Usages
There are several APIs you can use

+ Top 10 information:

        https://movie-crawler.herokuapp.com/api/v2/rank/(category).json

  You have three choices to replace the ```(category)``` part.
  1. use ```1``` to get the top 10 popular movies in U.S. area.
  2. use ```2``` to get the top 10 popular movies in Taiwan area.
  3. use ```3``` to get the top 10 popular DVD.

  *Ex:*
  If I would like to get the top ten popular movies in Taiwan, then I could access

          https://movie-crawler.herokuapp.com/api/v2/rank/2.json

  The result will reach to you just-in-time.

+ One more thing with Top 10:

  If you are impatient to see the whole ranking list and want to see the real best movie amont the three lists. You could have a "post" request to access the data.

  with

      curl -v -H "Accept: application/json" -H "Content-type: application/json" -X POST -d "{\"top\":3}"  http://movie-crawler.herokuapp.com/api/v2/checktop

  Only one command, the api will return you with the top 3 most popular movie in the three list all at once. Sounds interesting? Have a try!

+ To get informaiton of the latest or second round movies available in Taiwan:
  1. If you want take a look at the latest movies. Please access

          https://movie-crawler.herokuapp.com/api/v2/info/latest.json

  2. If you want know the informations of the second-round ones.

          https://movie-crawler.herokuapp.com/api/v2/info/seond_round.json

  new in v2

+ To get the specific information of some movie, including crew, comments and others:

  If you want check superman

        https://movie-crawler.herokuapp.com/api/v2/movie/superman.json
