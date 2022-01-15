package com.zuehlke.securesoftwaredevelopment.repository;

import com.zuehlke.securesoftwaredevelopment.config.AuditLogger;
import com.zuehlke.securesoftwaredevelopment.domain.DeliveryDetail;
import com.zuehlke.securesoftwaredevelopment.domain.ViewableDelivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class DeliveryRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerRepository.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(DeliveryRepository.class);

    private DataSource dataSource;

    public DeliveryRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<ViewableDelivery> getAllDeliveries() {
        List<ViewableDelivery> deliveries = new ArrayList<>();
        String sqlQuery = "SELECT d.id, d.isDone, d.date, d.comment, u.username, r.name, rt.name, a.name FROM delivery AS d JOIN users AS u ON d.userId = u.id JOIN restaurant as r ON d.restaurantId = r.id JOIN address AS a ON d.addressId = a.id JOIN restaurant_type AS rt ON r.typeId= rt.id";

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {

            while (rs.next()) {
                deliveries.add(createDelivery(rs));
            }
            LOG.info("Get all deliveries successful");

        } catch (SQLException e) {
            LOG.warn("Get customers failed", e);
        }
        return deliveries;
    }


    private ViewableDelivery createDelivery(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        boolean isDone = rs.getBoolean(2);
        Date date = rs.getDate(3);
        String comment = rs.getString(4);
        String username = rs.getString(5);
        String restaurantName = rs.getString(6);
        String restaurantType = rs.getString(7);
        String address = rs.getString(8);
        return new ViewableDelivery(id, isDone, date, comment, username, address, restaurantName, restaurantType);
    }

    public ViewableDelivery getDelivery(String id) {
        String sqlQuery = "SELECT d.id, d.isDone, d.date, d.comment, u.username, r.name, rt.name, a.name FROM delivery AS d JOIN users AS u ON d.userId = u.id JOIN restaurant as r ON d.restaurantId = r.id JOIN address AS a ON d.addressId = a.id JOIN restaurant_type AS rt ON r.typeId= rt.id WHERE d.id = " + id;

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {

            if (rs.next()) {
                return createDelivery(rs);
            }
            LOG.info("Get delivery id = " + id);

        } catch (SQLException e) {
            LOG.warn("Get delivery failed for id = " + id, e);
        }
        return null;
    }

    public List<DeliveryDetail> getDeliveryDetails(String id) {
        List<DeliveryDetail> details = new ArrayList<>();
        String sqlQuery = "SELECT di.id, di.amount, f.name, f.price FROM delivery_item AS di JOIN food AS f ON di.foodId = f.id WHERE deliveryId = " + id;

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {

            while (rs.next()) {
                details.add(createDetail(rs));
            }
            LOG.info("Get delivery details for id = " + id);

        } catch (SQLException e) {
            LOG.warn("Get delivery details failed for id = " + id, e);
        }
        return details;
    }


    private DeliveryDetail createDetail(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        int amount = rs.getInt(2);
        String foodName = rs.getString(3);
        int price = rs.getInt(4);

        return new DeliveryDetail(id, amount, foodName, price);

    }

    public int calculateSum(List<DeliveryDetail> details){
        int sum = 0;
        for(DeliveryDetail detail: details){
            sum+= detail.getPrice() * detail.getAmount();
        }
        return sum;
    }


    public List<ViewableDelivery> search(String searchQuery) {
        List<ViewableDelivery> cars = new ArrayList<>();
        String sqlQuery =
                "SELECT d.id, d.isDone, d.date, d.comment, u.username, r.name, rt.name, a.name FROM delivery AS d JOIN users AS u ON d.userId = u.id JOIN restaurant as r ON d.restaurantId = r.id JOIN address AS a ON d.addressId = a.id JOIN restaurant_type AS rt ON r.typeId= rt.id" +
                        " WHERE UPPER(d.comment) LIKE UPPER('%" + searchQuery + "%')"
                        + "OR UPPER(u.username) LIKE UPPER('%" + searchQuery + "%')"
                        + "OR UPPER(r.name) LIKE UPPER('%" + searchQuery + "%')"
                        + "OR UPPER(rt.name) LIKE UPPER('%" + searchQuery + "%')"
                        + "OR UPPER(a.name) LIKE UPPER('%" + searchQuery + "%')";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {
            while (rs.next()) {
                cars.add(createDelivery(rs));
            }
            LOG.info("Delivery search successful for searchQuery = " + searchQuery);
        }
        catch (SQLException e) {
            LOG.warn("Delivery search failed for searchQuery = " + searchQuery, e);
        }
        return cars;
    }

}
