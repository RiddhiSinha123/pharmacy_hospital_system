# 💊 Pharmacy Management System (Spring Boot)

A simple backend Pharmacy Management System built using Java Spring Boot and MySQL.

This project currently supports basic medicine inventory management features and is designed to be extended into a production-ready system in the future.

---

## 🚀 Current Features

1. ✅ Add Medicine  
   - Add medicine with:
     - Name
     - Stock
     - Expiry Date
     - Low Stock Threshold

2. ✅ Sell Medicine  
   - Reduces stock quantity
   - Prevents selling if stock is insufficient

3. ✅ Low Stock Alert  
   - Fetch medicines where stock is below a threshold

---

## 🛠️ Tech Stack

- Java 23
- Spring Boot
- Spring Data JPA
- MySQL
- Hibernate
- Maven
- IntelliJ IDEA

---

## 📂 Project Structure

src
└── main
├── java/com/example
│ ├── controller
│ ├── service
│ ├── repository
│ ├── model
│ └── PharmacyApplication.java
└── resources
└── application.properties


---

## ⚙️ Setup Instructions

### 1️⃣ Clone the Repository

## 🔮 Future Enhancements

The current version focuses on basic medicine inventory management.  
The following improvements are planned for future releases:

### 🔐 Authentication & Authorization
- Role-based access (Admin, Pharmacist)
- Secure login using Spring Security
- JWT-based authentication
- Password encryption using BCrypt

---

### 📦 Advanced Inventory Management
- Expiry date alerts (automatic detection of near-expiry medicines)
- Automatic low-stock email notifications
- Batch management for medicines
- Supplier management module
- Purchase history tracking

---

### 📊 Dashboard & Analytics
- Real-time stock dashboard
- Monthly sales reports
- Most sold medicines report
- Graph-based analytics using charts
- Revenue tracking

---

### 🧾 Billing & Invoice System
- Generate digital invoices (PDF)
- Store billing history
- GST/tax calculation support
- Customer purchase history

---

### 🔎 Search & Filtering
- Search medicine by name
- Filter by expiry date
- Filter by stock availability
- Pagination for large datasets

---

### 🧪 Testing & Code Quality
- Unit testing using JUnit
- Integration testing
- API testing automation
- Code coverage reports

---

### 🐳 Docker & Containerization
- Dockerize the Spring Boot application
- Docker Compose for MySQL + Backend
- Environment-based configuration

---

### 🚀 CI/CD & Blue-Green Deployment
- CI/CD pipeline using GitHub Actions
- Automated build and test on every push
- Blue-Green Deployment strategy:
  - Two identical environments (Blue & Green)
  - Load balancer-based traffic switching
  - Zero-downtime production deployment
  - Instant rollback capability
- Kubernetes-based deployment in future

---

### ☁️ Cloud Deployment
- Deploy to AWS / Azure / GCP
- Use managed database services (RDS)
- Implement monitoring using Prometheus & Grafana
- Centralized logging

---

### 📱 Frontend Integration
- React / Angular frontend
- Admin dashboard UI
- REST API integration
- Mobile-friendly UI

---

### 🔔 Notification System
- Email alerts for low stock
- SMS alerts for expiry
- In-app notifications

---

### 🔒 Security Enhancements
- Input validation
- Global exception handling
- API rate limiting
- HTTPS configuration
- CORS configuration
