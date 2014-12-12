require 'aws-sdk'

class Movie < AWS::Record::HashModel
  string_attr :moviename
  string_attr :movieinfo
  def self.destroy(id)
    find(id).destroy
  end

  def self.delete_all
    all.each { |r| r.delete}
  end
end
