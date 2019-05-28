package test.app1;

import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;

public class BookingEvent implements Serializable {
	public BookingEvent() {
		super();
		// TODO Auto-generated constructor stub
	}

	private static final long serialVersionUID = 1L;

	public BookingEvent(JsonNode docRoot) {
		super();
		this.date_time = docRoot.get("date_time").asText();
		this.site_name = docRoot.get("site_name").asInt();
		this.posa_continent = docRoot.get("posa_continent").asInt();
		this.user_location_country = docRoot.get("user_location_country").asInt();
		this.user_location_region = docRoot.get("user_location_region").asInt();
		this.user_location_city = docRoot.get("user_location_city").asInt();
		this.orig_destination_distance = docRoot.get("orig_destination_distance").asInt();
		this.user_id = docRoot.get("user_id").asInt();
		this.is_mobile = docRoot.get("is_mobile").asInt();
		this.is_package = docRoot.get("is_package").asInt();
		this.channel = docRoot.get("channel").asInt();
		this.srch_ci = docRoot.get("srch_ci").asText();
		this.srch_co = docRoot.get("srch_co").asText();
		this.srch_adults_cnt = docRoot.get("srch_adults_cnt").asInt();
		this.srch_children_cnt = docRoot.get("srch_children_cnt").asInt();
		this.srch_rm_cnt = docRoot.get("srch_rm_cnt").asInt();
		this.srch_destination_id = docRoot.get("srch_destination_id").asInt();
		this.srch_destination_type_id = docRoot.get("srch_destination_type_id").asInt();
		this.is_booking = docRoot.get("is_booking").asInt();
		this.cnt = docRoot.get("cnt").asInt();
		this.hotel_continent = docRoot.get("hotel_continent").asInt();
		this.hotel_country = docRoot.get("hotel_country").asInt();
		this.hotel_market = docRoot.get("hotel_market").asInt();
		this.hotel_cluster = docRoot.get("hotel_cluster").asInt();
	}

	private String date_time;
	private int site_name;
	private int posa_continent;
	private int user_location_country;
	private int user_location_region;
	private int user_location_city;
	private int orig_destination_distance;
	private int user_id;
	private int is_mobile;
	private int is_package;
	private int channel;
	private String srch_ci;
	private String srch_co;
	private int srch_adults_cnt;
	private int srch_children_cnt;
	private int srch_rm_cnt;
	private int srch_destination_id;
	private int srch_destination_type_id;
	private int is_booking;
	private int cnt;
	private int hotel_continent;
	private int hotel_country;
	private int hotel_market;
	private int hotel_cluster;

	public String getDate_time() {
		return date_time;
	}

	public void setDate_time(String date_time) {
		this.date_time = date_time;
	}

	public int getSite_name() {
		return site_name;
	}

	public void setSite_name(int site_name) {
		this.site_name = site_name;
	}

	public int getPosa_continent() {
		return posa_continent;
	}

	public void setPosa_continent(int posa_continent) {
		this.posa_continent = posa_continent;
	}

	public int getUser_location_country() {
		return user_location_country;
	}

	public void setUser_location_country(int user_location_country) {
		this.user_location_country = user_location_country;
	}

	public int getUser_location_region() {
		return user_location_region;
	}

	public void setUser_location_region(int user_location_region) {
		this.user_location_region = user_location_region;
	}

	public int getUser_location_city() {
		return user_location_city;
	}

	public void setUser_location_city(int user_location_city) {
		this.user_location_city = user_location_city;
	}

	public int getOrig_destination_distance() {
		return orig_destination_distance;
	}

	public void setOrig_destination_distance(int orig_destination_distance) {
		this.orig_destination_distance = orig_destination_distance;
	}

	public int getUser_id() {
		return user_id;
	}

	public void setUser_id(int user_id) {
		this.user_id = user_id;
	}

	public int getIs_mobile() {
		return is_mobile;
	}

	public void setIs_mobile(int is_mobile) {
		this.is_mobile = is_mobile;
	}

	public int getIs_package() {
		return is_package;
	}

	public void setIs_package(int is_package) {
		this.is_package = is_package;
	}

	public int getChannel() {
		return channel;
	}

	public void setChannel(int channel) {
		this.channel = channel;
	}

	public String getSrch_ci() {
		return srch_ci;
	}

	public void setSrch_ci(String srch_ci) {
		this.srch_ci = srch_ci;
	}

	public String getSrch_co() {
		return srch_co;
	}

	public void setSrch_co(String srch_co) {
		this.srch_co = srch_co;
	}

	public int getSrch_adults_cnt() {
		return srch_adults_cnt;
	}

	public void setSrch_adults_cnt(int srch_adults_cnt) {
		this.srch_adults_cnt = srch_adults_cnt;
	}

	public int getSrch_children_cnt() {
		return srch_children_cnt;
	}

	public void setSrch_children_cnt(int srch_children_cnt) {
		this.srch_children_cnt = srch_children_cnt;
	}

	public int getSrch_rm_cnt() {
		return srch_rm_cnt;
	}

	public void setSrch_rm_cnt(int srch_rm_cnt) {
		this.srch_rm_cnt = srch_rm_cnt;
	}

	public int getSrch_destination_id() {
		return srch_destination_id;
	}

	public void setSrch_destination_id(int srch_destination_id) {
		this.srch_destination_id = srch_destination_id;
	}

	public int getSrch_destination_type_id() {
		return srch_destination_type_id;
	}

	public void setSrch_destination_type_id(int srch_destination_type_id) {
		this.srch_destination_type_id = srch_destination_type_id;
	}

	public int getIs_booking() {
		return is_booking;
	}

	public void setIs_booking(int is_booking) {
		this.is_booking = is_booking;
	}

	public int getCnt() {
		return cnt;
	}

	public void setCnt(int cnt) {
		this.cnt = cnt;
	}

	public int getHotel_continent() {
		return hotel_continent;
	}

	public void setHotel_continent(int hotel_continent) {
		this.hotel_continent = hotel_continent;
	}

	public int getHotel_country() {
		return hotel_country;
	}

	public void setHotel_country(int hotel_country) {
		this.hotel_country = hotel_country;
	}

	public int getHotel_market() {
		return hotel_market;
	}

	public void setHotel_market(int hotel_market) {
		this.hotel_market = hotel_market;
	}

	public int getHotel_cluster() {
		return hotel_cluster;
	}

	public void setHotel_cluster(int hotel_cluster) {
		this.hotel_cluster = hotel_cluster;
	}

}
