package com.zuehlke.securesoftwaredevelopment.controller;

import com.zuehlke.securesoftwaredevelopment.config.AuditLogger;

import com.zuehlke.securesoftwaredevelopment.config.SecurityUtil;
import com.zuehlke.securesoftwaredevelopment.domain.*;
import com.zuehlke.securesoftwaredevelopment.repository.CustomerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpSession;
import java.util.List;

@Controller

public class CustomerController {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerController.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(CustomerController.class);

    private final CustomerRepository customerRepository;

    public CustomerController(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
    }

    @GetMapping("/customers-and-restaurants")
    public String customersAndRestaurants(Model model) {
        List<Customer> customers = customerRepository.getCustomers();
        if (!SecurityUtil.hasPermission("USERS_LIST_VIEW")) {
            customers.forEach(customer -> customer = null);
        }
        if (!SecurityUtil.hasPermission("USERS_DETAILS_VIEW")) {
            customers.forEach(customer -> customer.setPassword(null));
        }

        List<Restaurant> restaurants = customerRepository.getRestaurants();
        if (!SecurityUtil.hasPermission("RESTAURANT_LIST_VIEW")) {
            restaurants.forEach(restaurant -> restaurant = null);
        }
        if (!SecurityUtil.hasPermission("RESTAURANT_DETAILS_VIEW")) {
            restaurants.forEach(restaurant -> {
                restaurant.setAddress(null);
                restaurant.setRestaurantType(null);
            });
        }

        model.addAttribute("customers", customers);
        model.addAttribute("restaurants", restaurants);
        return "customers-and-restaurants";
    }

    @GetMapping("/restaurant")
    @PreAuthorize("hasAuthority('RESTAURANT_DETAILS_VIEW')")
    public String getRestaurant(@RequestParam(name = "id", required = true) String id, Model model) {
        model.addAttribute("restaurant", customerRepository.getRestaurant(id));
        return "restaurant";
    }

    @DeleteMapping("/restaurant")
    @PreAuthorize("hasAuthority('RESTAURANT_DELETE')")
    public String deleteRestaurant(@RequestParam(name = "id", required = true) String id) {
        int identificator = Integer.valueOf(id);
        customerRepository.deleteRestaurant(identificator);
        return "/customers-and-restaurants";
    }

    @PostMapping("/api/restaurant/update-restaurant")
    @PreAuthorize("hasAuthority('RESTAURANT_EDIT')")
    public String updateRestaurant(RestaurantUpdate restaurantUpdate, Model model) {
        customerRepository.updateRestaurant(restaurantUpdate);
        customersAndRestaurants(model);
        return "/customers-and-restaurants";
    }

    @GetMapping("/customer")
    @PreAuthorize("hasAuthority('USERS_DETAILS_VIEW')")
    public String getCustomer(@RequestParam(name = "id", required = true) String id, Model model, HttpSession httpSession) {
        String csrf = httpSession.getAttribute("CSRF_TOKEN").toString();
        model.addAttribute("CSRF_TOKEN", csrf);
        model.addAttribute("customer", customerRepository.getCustomer(id));
        model.addAttribute("addresses", customerRepository.getAddresses(id));
        return "customer";
    }

    @DeleteMapping("/customer")
    @PreAuthorize("hasAuthority('USERS_DELETE')")
    public String deleteCustomer(@RequestParam(name = "id", required = true) String id) {
        customerRepository.deleteCustomer(id);
        return "/customers-and-restaurants";
    }

    @PostMapping("/api/customer/update-customer")
    @PreAuthorize("hasAuthority('USERS_EDIT')")
    public String updateCustomer(CustomerUpdate customerUpdate, Model model, HttpSession httpSession,
                                 @RequestParam("csrfToken") String csrfToken) throws AccessDeniedException {
        String sessionToken = httpSession.getAttribute("CSRF_TOKEN").toString();
        if (!csrfToken.equals(sessionToken)) {
            throw new AccessDeniedException("Access forbiden!");
        }

        customerRepository.updateCustomer(customerUpdate);
        customersAndRestaurants(model);
        return "/customers-and-restaurants";
    }

    @DeleteMapping("/customer/address")
    public String deleteCustomerAddress(@RequestParam(name = "id", required = true) String id) {
        int identificator = Integer.valueOf(id);
        customerRepository.deleteCustomerAddress(identificator);
        return "/customers-and-restaurants";
    }

    @PostMapping("/api/customer/address/update-address")
    public String updateCustomerAddress(Address address, Model model) {
        customerRepository.updateCustomerAddress(address);
        customersAndRestaurants(model);
        return "/customers-and-restaurants";
    }

    @PostMapping("/customer/address")
    public String putCustomerAddress(NewAddress newAddress, Model model){
        customerRepository.putCustomerAddress(newAddress);
        customersAndRestaurants(model);
        return "/customers-and-restaurants";
    }
}