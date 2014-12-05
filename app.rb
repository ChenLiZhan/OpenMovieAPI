require 'sinatra/base'
require 'movie_crawler'
require 'json'
require 'sinatra/flash'
require 'yaml'
# require 'sinatra/namespace'
require 'haml'
require 'sinatra/simple-navigation'
require 'sinatra/flash'
require 'httparty'
require_relative 'model/movie'
require_relative 'model/theater'

# web version of MovieCrawlerApp(https://github.com/ChenLiZhan/SOA-Crawler)
class MovieCrawlerApp < Sinatra::Base
  set :views, Proc.new { File.join(root, "views") }
  # enable :sessions
  use Rack::Session::Pool
  register Sinatra::Flash
  use Rack::MethodOverride
  register Sinatra::SimpleNavigation
  SimpleNavigation.config_file_paths << File.expand_path('../config', __FILE__)
  # register Sinatra::Namespace

  configure :production, :development do
    enable :logging
  end

  API_BASE_URI = 'http://localhost:4567'

  helpers do
    # RANK_LIST = { '1' => 'U.S.', '2' => 'Taiwan', '3' => 'DVD' }

    def get_movie_info(moviename)
      # begin
        # halt 404 if moviename == nil?
        movie_crawled={
          'type' => 'movie_info',
          'info' => []
        }
        movie_crawled['info'] = MovieCrawler.get_movie_info(moviename)
        movie_crawled
    end

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
  end

  after { ActiveRecord::Base.connection.close }

  get '/' do
    haml :home
  end

  get '/info/:category' do
    @intros = get_infos(params[:category])

    haml :intro
  end

  get '/rank/:category' do
    @boxoffices = get_ranks(params[:category])

    haml :boxoffice
  end

  get '/movie' do
    @action = :create
    haml :movie
  end

  post '/movie' do
    # if params[:movie].split(/[\r\n\s]/).length > 1
    #   flash[:notice] = 'One movie at a time, take easy!'
    #   redirect '/movie'
    #   return nil
    # end

    movie = params[:movie].strip
    request_url = "#{API_BASE_URI}/api/v2/movie"
    param = {
      movie: movie
    }
    options = { 
      headers: { 'Content-Type' => 'application/json' },
      body: param.to_json
    }
    result = HTTParty.post(request_url, options)

    # if (result.code != 200 && result.code != 302)
    #   flash[:notice] = 'Movie not found'
    #   redirect '/movie'
    #   return nil
    # end

    id = result.request.last_uri.path.split('/').last
    session[:result] = result.to_json
    session[:movie] = movie
    session[:action] = :create
    redirect "/movie/#{id}"
  end

  get '/movie/:id' do
    # if session[:action] == :create
    #   @results = JSON.parse(session[:result])
    #   @movie = session[:movie]
    # else
    request_url = "#{API_BASE_URI}/api/v2/moviechecked/#{params[:id]}"
    result = HTTParty.get(request_url)
    @results = YAML.load(result['info'])
    # end

    @id = params[:id]
    @action = :update
    haml :movie
  end

  delete '/movie/:id' do
    request_url = "#{API_BASE_URI}/api/v2/moviechecked/#{params[:id]}"
    result = HTTParty.delete(request_url)
    flash[:notice] = 'Record of movie deleted'
    redirect '/movie'
  end

  # # namespace '/api/v1' do

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
    # movie = Movie.find_by(moviename: params[:name])
    # if movie
    #   # return "find"+params[:name]
    #   # redirect "/api/v2/moviechecked/#{params[:name]}"
    #   movie.movieinfo
    # else
    #   movie = Movie.new
    #   movie.moviename = params[:name]
    #   movie.movieinfo = get_movie_info(params[:name]).to_json
    #   movie.save
    #   movie.movieinfo
    # end
    movie = Movie.find_by(moviename: req['movie'])
    if movie.nil?
      movie = Movie.new
      movie.moviename = req['movie']
      movie.movieinfo = get_movie_info(req['movie']).to_json
      movie.save
    end

    redirect "/api/v2/moviechecked/#{movie.id}"
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

    if @data = Theater.find_by(category: params[:category])
      @data = {
        'content_type' => @data.content_type,
        'category' => @data.category,
        'info' => JSON.parse(@data.content)
      }
      @data.to_json
    else
      data = params[:type] == 'info' ? get_infos(params[:category]) : \
      get_ranks(params[:category])
      theater = Theater.new
      theater.content_type = data['content_type']
      theater.category = data['category']
      theater.content = data['content'].to_json
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

  get '/info/' do
    halt 400
  end
  # end
end
