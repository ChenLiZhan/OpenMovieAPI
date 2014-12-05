require 'sinatra'
require 'sinatra/activerecord'
require_relative '../config/environments'

class Theater < ActiveRecord::Base
end
