package com.example.controller;

import org.springframework.web.bind.annotation.*;
import java.util.List;

import com.example.model.Medicine;
import com.example.service.MedicineService;

@RestController
@RequestMapping("/api/medicine")
public class MedicineController {

    private final MedicineService service;

    public MedicineController(MedicineService service) {
        this.service = service;
    }

    //  Add Medicine
    @PostMapping("/add")
    public Medicine addMedicine(@RequestBody Medicine medicine) {
        return service.addMedicine(medicine);
    }

    //  Sell Medicine
    @PutMapping("/sell/{id}")
    public Medicine sellMedicine(@PathVariable Long id,
                                 @RequestParam int qty) {
        return service.sellMedicine(id, qty);
    }

    // Low Stock Alert
    @GetMapping("/low-stock")
    public List<Medicine> lowStock(@RequestParam int threshold) {
        return service.getLowStockMedicines(threshold);
    }
}
