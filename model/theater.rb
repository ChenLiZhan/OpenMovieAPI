require 'aws-sdk'

class Theater < AWS::Record::HashModel
  string_attr :content_type
  string_attr :category
  string_attr :content

  def self.destroy(id)
    find(id).delete
  end

  def self.delete_all
    all.each { |r| r.delete }
  end
end
