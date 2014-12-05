require 'sinatra'
require 'sinatra/activerecord'
require_relative '../config/environments'

class Movie < ActiveRecord::Base
end
