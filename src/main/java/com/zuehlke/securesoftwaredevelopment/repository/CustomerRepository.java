package com.zuehlke.securesoftwaredevelopment.repository;

import com.zuehlke.securesoftwaredevelopment.config.AuditLogger;
import com.zuehlke.securesoftwaredevelopment.config.Entity;
import com.zuehlke.securesoftwaredevelopment.config.SecurityUtil;
import com.zuehlke.securesoftwaredevelopment.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class CustomerRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerRepository.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(CustomerRepository.class);

    private DataSource dataSource;

    public CustomerRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Person createPersonFromResultSet(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String firstName = rs.getString(2);
        String lastName = rs.getString(3);
        String personalNumber = rs.getString(4);
        String address = rs.getString(5);
        return new Person(id, firstName, lastName, personalNumber, address);
    }

    public List<Customer> getCustomers() {
        List<com.zuehlke.securesoftwaredevelopment.domain.Customer> customers = new ArrayList<com.zuehlke.securesoftwaredevelopment.domain.Customer>();
        String query = "SELECT id, username FROM users";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                customers.add(createCustomer(rs));
            }
            LOG.info("Get customers successful");
        } catch (SQLException e) {
            LOG.warn("Get customers failed", e);
        }
        return customers;
    }

    private com.zuehlke.securesoftwaredevelopment.domain.Customer createCustomer(ResultSet rs) throws SQLException {
        return new com.zuehlke.securesoftwaredevelopment.domain.Customer(rs.getInt(1), rs.getString(2));
    }

    public List<Restaurant> getRestaurants() {
        List<Restaurant> restaurants = new ArrayList<Restaurant>();
        String query = "SELECT r.id, r.name, r.address, rt.name  FROM restaurant AS r JOIN restaurant_type AS rt ON r.typeId = rt.id ";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                restaurants.add(createRestaurant(rs));
            }
            LOG.info("Get restaurants successful");
        } catch (SQLException e) {
            LOG.warn("Get restaurants failed", e);
        }
        return restaurants;
    }

    private Restaurant createRestaurant(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        String address = rs.getString(3);
        String type = rs.getString(4);

        return new Restaurant(id, name, address, type);
    }


    public Object getRestaurant(String id) {
        String query = "SELECT r.id, r.name, r.address, rt.name  FROM restaurant AS r JOIN restaurant_type AS rt ON r.typeId = rt.id WHERE r.id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            if (rs.next()) {
                return createRestaurant(rs);
            }
            LOG.info("Get restaurant details for id = " + id);
        } catch (SQLException e) {
            LOG.warn("Restaurant search failed for id = " + id, e);
        }
        return null;
    }

    public void deleteRestaurant(int id) {
        String query = "DELETE FROM restaurant WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            auditLogger.audit("Deleted restaurant id = " + id);
        } catch (SQLException e) {
            LOG.warn("Restaurant delete failed for id = " + id, e);
        }
    }

    public void updateRestaurant(RestaurantUpdate restaurantUpdate) {
        Restaurant restaurantFromDb = (Restaurant) getRestaurant(String.valueOf(restaurantUpdate.getId()));
        String query = "UPDATE restaurant SET name = '" + restaurantUpdate.getName() + "', address='" + restaurantUpdate.getAddress() + "', typeId =" + restaurantUpdate.getRestaurantType() + " WHERE id =" + restaurantUpdate.getId();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);

            boolean nameChanged = false, addressChanged = false;
            if (!restaurantFromDb.getName().equals(restaurantUpdate.getName())) {
                auditLogger.auditChange(new Entity(
                        "Restaurant name change",
                        String.valueOf(restaurantFromDb.getId()),
                        String.valueOf(restaurantFromDb.getName()),
                        String.valueOf(restaurantUpdate.getName())
                ));
                nameChanged = true;
            }
            if (!restaurantFromDb.getAddress().equals(restaurantUpdate.getAddress())) {
                auditLogger.auditChange(new Entity(
                        "Restaurant address change",
                        String.valueOf(restaurantFromDb.getId()),
                        String.valueOf(restaurantFromDb.getAddress()),
                        String.valueOf(restaurantUpdate.getAddress())
                ));
                addressChanged = true;
            }
            if (!nameChanged && !addressChanged) {
                auditLogger.audit("Restaurant type change");
            }
        } catch (SQLException e) {
            LOG.warn("Restaurant update failed for name = " + restaurantUpdate.getName(), e);
        }
    }

    public Customer getCustomer(String id) {
        String sqlQuery = "SELECT id, username, password FROM users WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {

            if (rs.next()) {
                return createCustomerWithPassword(rs);
            }
            LOG.info("Get customer details for id = " + id);

        } catch (SQLException e) {
            LOG.warn("Customer search failed for id = " + id, e);
        }
        return null;
    }

    private Customer createCustomerWithPassword(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String username = rs.getString(2);
        String password = rs.getString(3);
        return new Customer(id, username, password);
    }


    public void deleteCustomer(String id) {
        String query = "DELETE FROM users WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            auditLogger.audit("Deleted customer id = " + id);
        } catch (SQLException e) {
            LOG.warn("Customer delete failed for id = " + id, e);
        }
    }

    public void updateCustomer(CustomerUpdate customerUpdate) {
        String query = "UPDATE users SET username = '" + customerUpdate.getUsername() + "', password='" + customerUpdate.getPassword() + "' WHERE id =" + customerUpdate.getId();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            auditLogger.audit("Updated customer username = " + customerUpdate.getUsername());
        } catch (SQLException e) {
            LOG.warn("Customer update failed for username = " + customerUpdate.getUsername(), e);
        }
    }

    public List<Address> getAddresses(String id) {
        String sqlQuery = "SELECT id, name FROM address WHERE userId=" + id;
        List<Address> addresses = new ArrayList<Address>();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {

            while (rs.next()) {
                addresses.add(createAddress(rs));
            }
            LOG.info("Get user addresses for userId = " + id);

        } catch (SQLException e) {
            LOG.warn("Address search failed for id = " + id, e);
        }
        return addresses;
    }

    private Address createAddress(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        return new Address(id, name);
    }

    public void deleteCustomerAddress(int id) {
        String query = "DELETE FROM address WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            auditLogger.audit("Deleted address id = " + id);
        } catch (SQLException e) {
            LOG.warn("Address delete failed for id = " + id, e);
        }
    }

    public void updateCustomerAddress(Address address) {
        String query = "UPDATE address SET name = '" + address.getName() + "' WHERE id =" + address.getId();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            auditLogger.audit("Address name change for id = " + address.getId());
        } catch (SQLException e) {
            LOG.warn("Address update failed for name = " + address.getName(), e);
        }
    }

    public void putCustomerAddress(NewAddress newAddress) {
        String query = "INSERT INTO address (name, userId) VALUES ('"+newAddress.getName()+"' , "+newAddress.getUserId()+")";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            auditLogger.audit("Inserted address name = " + newAddress.getName());
        } catch (SQLException e) {
            LOG.warn("Address insert failed for name = " + newAddress.getName(), e);
        }
    }
}
