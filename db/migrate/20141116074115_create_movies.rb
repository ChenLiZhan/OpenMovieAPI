class CreateMovies < ActiveRecord::Migration
  def self.up
    create_table :movies do |m|
      m.text :moviename
      m.text :movieinfo
    end
  end

  def self.down
    drop_table :movies
  end
end
