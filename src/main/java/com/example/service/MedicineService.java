package com.example.service;

import org.springframework.stereotype.Service;
import java.util.List;

import com.example.model.Medicine;
import com.example.repository.MedicineRepository;

@Service
public class MedicineService {

    private final MedicineRepository repository;

    public MedicineService(MedicineRepository repository) {
        this.repository = repository;
    }


    // Add Medicine

    public Medicine addMedicine(Medicine medicine) {
        return repository.save(medicine);
    }


    //  Sell Medicine (Reduce Stock)

    public Medicine sellMedicine(Long id, int qty) {

        Medicine medicine = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Medicine not found"));

        medicine.reduceStock(qty);

        return repository.save(medicine);
    }


    // ️Low Stock Alert

    public List<Medicine> getLowStockMedicines(int threshold) {
        return repository.findByStockLessThanEqual(threshold);
    }
}
