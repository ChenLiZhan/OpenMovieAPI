require_relative 'spec_helper'
require_relative 'support/debut_helpers'
require 'json'
require_relative '../model/movie'
require_relative '../model/theater'

describe 'MovieCrawler debut' do

  include DebutHelpers

  # check routes
  describe 'Getting the root of MovieCrawler' do
    it 'should return ok' do
      get '/'
      last_response.must_be :ok?
    end
  end

  # check movie_info
  describe 'Checking get_movie_info' do

    # it 'shoule find checked movies' do
    #   if Movie.find_by(moviename: 'spiderman')
    #     get 'api/v2/movie/spiderman.json'
    #     last_response.must_be :ok?
    #   else
    #     get '/api/v2/movie/spiderman.json'
    #     last_response.must_be :ok?
    #   end
    # end


    it 'should return 404 for unknown movies' do
      get "api/v2/movie/.json"
      last_response.must_be :not_found?
    end

  end

  # methods get_ranks and get_infos
  describe 'Getting the rank and info' do
    before do
      Theater.delete_all
    end

    it 'should return ok and json format' do
      get "/api/v2/rank/#{rand(1..3)}.json"
      last_response.must_be :ok?
      data = last_response.body
      saved_theater = Theater.find_by(content_type: 'rank_table')
      %w(1 2 3).must_include saved_theater[:category]
      JSON.parse(data)['content'].must_equal JSON.parse(saved_theater[:content])
    end

    it 'should return 404 for unknown category' do
      get "/api/v2/rank/#{rand(4..100)}.json"
      last_response.must_be :not_found?
      Theater.find_by(content_type: 'rank_table').must_be_nil
    end

    it 'should return ok and json format' do
      get "/api/v2/info/#{info_helper.sample}.json"
      last_response.must_be :ok?
      data = last_response.body
      saved_theater = Theater.find_by(content_type: 'info_list')
      info_helper.must_include saved_theater[:category]
      JSON.parse(data)['content'].must_equal JSON.parse(saved_theater[:content])
    end

    it 'should return bad request if not specify category' do
      get '/api/v2/info/'
      last_response.must_be :not_found?
      Theater.find_by(content_type: 'info_list').must_be_nil
    end
  end

  # method topsum
  describe 'Checking the top n among three rank' do
    it 'should return ok and json format' do
      header = { 'Content-type' => 'application/json' }
      body = { top: 3 }

      post '/api/v2/checktop', body.to_json, header
      last_response.must_be :ok?
      last_response.must_be_instance_of Rack::MockResponse
    end

    it 'should return 404 for n other than 1..10' do
      header = { 'Content-type' => 'application/json' }
      body = { top: rand(11..100) }

      post '/api/v2/checktop', body.to_json, header
      last_response.must_be :not_found?
    end

    it 'should return 400 for bad JSON format' do
      header = { 'Content-type' => 'application/json' }
      body = {}

      post '/api/v2/checktop', body.to_json, header
      last_response.must_be :bad_request?
    end
  end
end
