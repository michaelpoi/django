from django.core.exceptions import FieldError
from django.db import connection
from django.db.models import (
    CharField,
    Avg, Count, DecimalField, DurationField, F, FloatField, Func, IntegerField,
    Max, Min, Sum, Value,
)
from django.db.models.expressions import Case, Exists, OuterRef, Subquery, With, When
from django.test import TestCase
from django.test.testcases import skipUnlessDBFeature
from django.test.utils import Approximate, CaptureQueriesContext
from django.utils.timezone import datetime, timedelta

from .models import Distributor, Film


class year(datetime):
    def __new__(cls, year):
        return super().__new__(cls, year, 1, 1)

class CTETestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        d1 = Distributor.objects.create(name='20th Century Fox')

        Film.objects.create(title='Airplane Sierra', duration=timedelta(minutes=62), release_year=year(2006), distributor=d1)
        Film.objects.create(title='Blackout Private', duration=timedelta(minutes=85), release_year=year(2009), distributor=d1)
        Film.objects.create(title='Bugsy Song', duration=timedelta(minutes=119), release_year=year(2006), distributor=d1)
        Film.objects.create(title='Christmas Moonshine', duration=timedelta(minutes=150), release_year=year(2005), distributor=d1)
        Film.objects.create(title='Destiny Saturday', duration=timedelta(minutes=56), release_year=year(2007), distributor=d1)
        Film.objects.create(title='Flamingos Connecticut', duration=timedelta(minutes=80), release_year=year(2003), distributor=d1)
        Film.objects.create(title='Gentlemen Stage', duration=timedelta(minutes=55), release_year=year(2011), distributor=d1)
        Film.objects.create(title='Hollywood Anonymous', duration=timedelta(minutes=69), release_year=year(2009), distributor=d1)
        Film.objects.create(title='Impossible Prejudice', duration=timedelta(minutes=103), release_year=year(2017), distributor=d1)
        Film.objects.create(title='Jason Trap', duration=timedelta(minutes=130), release_year=year(2013), distributor=d1)
        Film.objects.create(title='King Evolution', duration=timedelta(minutes=184), release_year=year(2012), distributor=d1)
        Film.objects.create(title='Lebowski Soldiers', duration=timedelta(minutes=59), release_year=year(2018), distributor=d1)
        Film.objects.create(title='Meet Chocolate', duration=timedelta(minutes=80), release_year=year(2008), distributor=d1)
        Film.objects.create(title='Punk Divorce', duration=timedelta(minutes=101), release_year=year(2005), distributor=d1)
        Film.objects.create(title='Rock Instinct', duration=timedelta(minutes=73), release_year=year(2010), distributor=d1)

    def t_est_case_when(self):
        """
        SELECT film_id, title, (CASE
            WHEN length < 60 THEN 'short'
            WHEN length >= 60 AND length <= 100 THEN 'regular'
            WHEN length > 100 THEN 'long'
        END) length FROM film
            WHERE length='short'
        """
        print('=== Test Case When ===')
        qs = Film.objects.annotate(
            length=Case(
                When(duration__lt=timedelta(minutes=60), then=Value('short')),
                When(duration__gte=timedelta(minutes=60), duration__lte=timedelta(minutes=100), then=Value('regular')),
                When(duration__gt=timedelta(minutes=100), then=Value('long')),
                output_field=CharField(),
            )
        ).values_list('title', 'length').filter(length='short')
        self.assertIs(qs.count(), 3)
        sql, params = qs.query.sql_with_params()
        sql = sql % params
        print(sql)

    def t_est_related_query(self):
        """
        SELECT film_id, title, (CASE
            WHEN length < 60 THEN 'short'
            WHEN length >= 60 AND length <= 100 THEN 'regular'
            WHEN length > 100 THEN 'long'
        END) length FROM film
                    INNER JOIN distributor ON (film.distributor_id=distributor.id)
            WHERE distributor WHERE distributor.name='20th Century Fox'
        """
        print('=== Test Related Query ===')
        qs = Film.objects.annotate(
            length=Case(
                When(duration__lt=timedelta(minutes=60), then=Value('short')),
                When(duration__gte=timedelta(minutes=60), duration__lte=timedelta(minutes=100), then=Value('regular')),
                When(duration__gt=timedelta(minutes=100), then=Value('long')),
                output_field=CharField(),
            )
        ).values_list('title', 'length')
        qs = qs.filter(distributor__name='20th Century Fox')
        self.assertIs(qs.count(), 15)
        sql, params = qs.query.sql_with_params()
        sql = sql % params
        print(sql)

    def test_non_recursive_cte(self):
        """
        WITH cte_film AS (
            SELECT film_id, title, (CASE
                WHEN length < 60 THEN 'short'
                WHEN length >= 60 AND length <= 100 THEN 'regular'
                WHEN length > 100 THEN 'long'
            END) length FROM film
        )
        SELECT id, title FROM cte_film;
        """
        print('=== Test Non-Recursive CTE ===')
        inner_qs = Film.objects.annotate(
            length=Case(
                When(duration__lt=timedelta(minutes=60), then=Value('short')),
                When(duration__gte=timedelta(minutes=60), duration__lte=timedelta(minutes=100), then=Value('regular')),
                When(duration__gt=timedelta(minutes=100), then=Value('long')),
                output_field=CharField(),
            )
        ).values_list('length')
        outer_qs = Film.objects.annotate(cte_film=With(inner_qs)).only('title')
        self.assertIs(outer_qs.count(), 15)
        sql, params = outer_qs.query.sql_with_params()
        sql = sql % params
        print(sql)

    def t_est_non_recursive_cte_with_filter(self):
        """
        WITH cte_film AS (
            SELECT film_id, title, (CASE
                WHEN length < 60 THEN 'short'
                WHEN length >= 60 AND length <= 100 THEN 'regular'
                WHEN length > 100 THEN 'long'
            END) length FROM film
        )
        SELECT * FROM cte_film WHERE length='short'
        """
        print('=== Test Non-Recursive CTE with filter ===')
        inner_qs = Film.objects.annotate(
            length=Case(
                When(duration__lt=timedelta(minutes=60), then=Value('short')),
                When(duration__gte=timedelta(minutes=60), duration__lte=timedelta(minutes=100), then=Value('regular')),
                When(duration__gt=timedelta(minutes=100), then=Value('long')),
                output_field=CharField(),
            )
        ).values_list('title', 'length')
        outer_qs = Film.objects.attach(cte_film=inner_qs)
        outer_qs = outer_qs.filter(cte_film__length='short')
        self.assertIs(outer_qs.count(), 15)
        sql, params = outer_qs.query.sql_with_params()
        sql = sql % params
        print(sql)
