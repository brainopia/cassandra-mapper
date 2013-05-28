class SimpleUUID::UUID
  def to_time
    time = Time.at(total_usecs / 1_000_000, total_usecs % 1_000_000)
    time.uuid = self
    time
  end
end
