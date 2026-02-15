package com.example.model;

import jakarta.persistence.*;
import java.time.LocalDate;

@Entity
public class Medicine {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;
    private int stock;
    private LocalDate expiryDate;
    private int lowStockLevel;

    // Default constructor (required by JPA)
    public Medicine() {
    }

    // Constructor
    public Medicine(String name, int stock, LocalDate expiryDate, int lowStockLevel) {
        this.name = name;
        this.stock = stock;
        this.expiryDate = expiryDate;
        this.lowStockLevel = lowStockLevel;
    }

    // -------------------------
    // Business Logic Methods
    // -------------------------

    public void reduceStock(int qty) {
        if (qty <= 0) {
            throw new RuntimeException("Quantity must be positive");
        }

        if (qty > this.stock) {
            throw new RuntimeException("Not enough stock available");
        }

        this.stock -= qty;
    }

    public boolean isLowStock() {
        return this.stock <= this.lowStockLevel;
    }

    public boolean isExpired() {
        return LocalDate.now().isAfter(this.expiryDate);
    }

    // -------------------------
    // Getters and Setters
    // -------------------------

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getStock() {
        return stock;
    }

    public LocalDate getExpiryDate() {
        return expiryDate;
    }

    public int getLowStockLevel() {
        return lowStockLevel;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setStock(int stock) {
        this.stock = stock;
    }

    public void setExpiryDate(LocalDate expiryDate) {
        this.expiryDate = expiryDate;
    }

    public void setLowStockLevel(int lowStockLevel) {
        this.lowStockLevel = lowStockLevel;
    }
}
