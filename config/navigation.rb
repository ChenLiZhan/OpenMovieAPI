SimpleNavigation::Configuration.run do |navigation|
  navigation.items do |primary|
    primary.item :info, 'Home', '/'
    primary.item :intro, 'Intro' do |intro|
      intro.item :latest, 'Latest', '/info/latest'
      intro.item :second_round, 'Second_Round', '/info/second_round'
    end
    primary.item :boxoffice, 'Box_Office' do |boxoffice|
      boxoffice.item :us, 'U.S', '/rank/1'
      boxoffice.item :taiwan, 'Taiwan', '/rank/2'
      boxoffice.item :dvd, 'DVD', '/rank/3'
    end
    primary.item :info, 'Info' do |info|
      info.item :search, 'Search for movie', '/movie'
    end
  end
end
