package com.syssniper.jsonschemakvstore.repository;

import com.syssniper.jsonschemakvstore.entity.InsurancePlan;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface ProjectElasticsearchRepository extends ElasticsearchRepository<InsurancePlan, String> {
    // Find insurance plans by a specific organization
    List<InsurancePlan> findBy_org(String org);

    // Find insurance plans by plan type
    List<InsurancePlan> findByPlanType(String planType);

    // Find insurance plans by creation date range
    List<InsurancePlan> findByCreationDateBetween(Date startDate, Date endDate);

    // Find insurance plans by linked service name
    List<InsurancePlan> findByLinkedPlanServicesLinkedServiceName(String serviceName);

    // Find insurance plans by plan service cost share deductible
    List<InsurancePlan> findByLinkedPlanServicesPlanserviceCostSharesDeductible(Integer deductible);

    // Find insurance plans by plan service cost share copay
    List<InsurancePlan> findByLinkedPlanServicesPlanserviceCostSharesCopay(Integer copay);
}
