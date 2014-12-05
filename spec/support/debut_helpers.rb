# some functions and lists to help app_spec
module DebutHelpers
  def info_helper
    %w(latest second_round) # ["latest", "second_round"]
  end

  def random_str(n)
    (0..n).map { ('a'..'z').to_a[rand(26)] }.join
  end
end
