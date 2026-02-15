package com.example.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.example.model.Medicine;
import java.util.List;

public interface MedicineRepository extends JpaRepository<Medicine, Long> {

    List<Medicine> findByStockLessThanEqual(int stock);
}
