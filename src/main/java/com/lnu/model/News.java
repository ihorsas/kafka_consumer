package com.lnu.model;

import java.sql.Timestamp;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "news", schema = "lnu-news")
public class News {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private int id;
	@Column(name = "title")
	private String title;
	@Column(name = "author")
	private String author;
	@Column(name = "views")
	private int views;
	@Column(name = "publish_date")
	private Timestamp publishDate;

	public News() {
	}

	public News(String title, String author, int views, Timestamp publishDate) {
		this.title = title;
		this.author = author;
		this.views = views;
		this.publishDate = publishDate;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public int getViews() {
		return views;
	}

	public void setViews(int views) {
		this.views = views;
	}

	public Timestamp getPublishDate() {
		return publishDate;
	}

	public void setPublishDate(Timestamp publishDate) {
		this.publishDate = publishDate;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		News that = (News) o;
		return id == that.id &&
				views == that.views &&
				Objects.equals(title, that.title) &&
				Objects.equals(author, that.author) &&
				Objects.equals(publishDate, that.publishDate);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, title, author, views, publishDate);
	}
}
