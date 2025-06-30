package uk.gov.justice.services.cakeshop.it.params;

import uk.gov.justice.services.cakeshop.it.helpers.SystemPropertyFinder;

public class CakeShopUris {
    private static final String HOST = "http://localhost:" + SystemPropertyFinder.findWildflyHttpPort();

    public static final String RECIPES_RESOURCE_URI = HOST + "/cakeshop-command-api/command/api/rest/cakeshop/recipes/";
    public static final String ORDERS_RESOURCE_URI = HOST + "/cakeshop-command-api/command/api/rest/cakeshop/orders/";
    public static final String RECIPES_RESOURCE_QUERY_URI = HOST + "/cakeshop-query-api/query/api/rest/cakeshop/recipes/";
    public static final String ORDERS_RESOURCE_QUERY_URI = HOST + "/cakeshop-query-api/query/api/rest/cakeshop/orders/";
    public static final String CAKES_RESOURCE_QUERY_URI = HOST + "/cakeshop-query-api/query/api/rest/cakeshop/cakes/";
    public static final String OVEN_RESOURCE_CUSTOM_URI = HOST + "/cakeshop-custom-api/custom/api/rest/cakeshop/ovens/";
    public static final String INDEXES_RESOURCE_QUERY_URI = HOST + "/cakeshop-query-api/query/api/rest/cakeshop/index/";
    public static final String HEALTHCHECK_URI = HOST + "/cakeshop-service/internal/healthchecks/all";
    public static final String STREAMS_QUERY_BASE_URI = HOST + "/cakeshop-service/internal/streams";
    public static final String STREAMS_QUERY_BY_ERROR_HASH_URI_TEMPLATE = STREAMS_QUERY_BASE_URI + "?errorHash=%s";
    public static final String STREAMS_QUERY_BY_STREAM_ID_URI_TEMPLATE = STREAMS_QUERY_BASE_URI + "?streamId=%s";
    public static final String STREAMS_QUERY_BY_HAS_ERROR = STREAMS_QUERY_BASE_URI + "?hasError=true";
    public static final String CAKES_RESOURCE_URI_FORMAT = RECIPES_RESOURCE_URI + "%s/cakes/%s";
}
