# select top 3 most popular countries where booking is successful (booking = 1)
SELECT hotel_country, count(hotel_country) as destination_country_count 
FROM train
WHERE is_booking=1
GROUP BY hotel_country
SORT BY destination_country_count DESC
LIMIT 3;