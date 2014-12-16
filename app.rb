require 'sinatra/base'
require 'movie_crawler'
require 'json'
require 'yaml'
require_relative 'model/movie'
require_relative 'model/theater'

class MovieAppDynamo < Sinatra::Base
  set :views, Proc.new { File.join(root, "views") }
  # enable :sessions
  use Rack::Session::Pool
  use Rack::MethodOverride

  # - requires config:
  # - create ENV vars AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION
  configure :production, :development do
    enable :logging
  end

  helpers do
    # RANK_LIST = { '1' => 'U.S.', '2' => 'Taiwan', '3' => 'DVD' }

    # return one movie info
    def get_movie_info(moviename)
      # begin
      # halt 404 if moviename == nil?
      movie_crawled = {
        'type' => 'movie_info',
        'info' => []
      }
      movie_crawled['info'] = MovieCrawler.get_movie_info(moviename)
      movie_crawled
    end

    # return a theater info
    def get_ranks(category)
      halt 404 if category.to_i > 3
      ranks_after = {
        'content_type' => 'rank_table',
        'category' => category,
        'content' => []
      }

      ranks_after['content'] = MovieCrawler.get_table(category)
      ranks_after
    end

    # return a theater info
    def get_infos(category)
      halt 404 if category == nil?
      infos_after = {
        'content_type' => 'info_list',
        'category' => category,
        'content' => []
      }

      infos_after['content'] = MovieCrawler.movies_parser(category)
      infos_after
    end

    # get a multiple info.
    def topsum(n)
      us1 = YAML.load(MovieCrawler::us_weekend).reduce(&:merge)
      tp1 = YAML.load(MovieCrawler::taipei_weekend).reduce(&:merge)
      dvd1 = YAML.load(MovieCrawler::dvd_rank).reduce(&:merge)
      keys = [us1, tp1, dvd1].flat_map(&:keys).uniq
      keys = keys[0, n]

      keys.map! do |k|
        { k => [{us:us1[k] || "0" }, { tp:tp1[k] || "0" }, { dvd:dvd1[k] || "0"}] }
      end
    end

    def new_movie(req)
      movie = Movie.new
      movie.moviename = req['movie'].to_json
      movie.movieinfo = get_movie_info(req['movie']).to_json
      movie
    end

    def new_theater(data)
      theater = Theater.new
      theater.content_type = data['content_type']
      theater.category = data['category']
      theater.content = data['content'].to_json
      theater
    end

  end

  # after { ActiveRecord::Base.connection.close }

  get '/' do
    'The API are working.'
  end

  post '/api/v2/movie' do
    content_type :json, charset: 'utf-8'

    body = request.body.read
    begin
      req = JSON.parse(body)
      logger.info req
    rescue Exception => e
      puts e.message
      halt 400
    end

    movie = new_movie(req)
    redirect "/api/v2/moviechecked/#{movie.id}" if movie.save
  end

  get '/api/v2/moviechecked/:id' do
    content_type :json, charset: 'utf-8'

    movie = Movie.find(params[:id])
    logger.info "result: #{movie.movieinfo}\n"
    movie.movieinfo
  end

  delete '/api/v2/moviechecked/:id' do
    Movie.destroy(params[:id])
  end

  get '/api/v2/:type/:category.json' do
    content_type :json, charset: 'utf-8'
    Theater.find(:all).each do |theater|
      theater.category == params[:category] && @data = theater
    end
    if @data.nil?
      @data = {
        'content_type' => @data.content_type,
        'category' => @data.category,
        'content' => JSON.parse(@data.content)
      }
      @data.to_json
    else
      data = params[:type] == 'info' ? get_infos(params[:category]) : \
      get_ranks(params[:category])
      theater = new_theater(data)
      theater.save && data.to_json
    end
  end

  post '/api/v2/checktop' do
    content_type :json, charset: 'utf-8'
    req = JSON.parse(request.body.read)
    n = req['top']
    halt 400 unless req.any?
    halt 404 unless [*1..10].include? n
    topsum(n).to_json
  end

  # get '/info/' do
  #   halt 400
  # end
end
