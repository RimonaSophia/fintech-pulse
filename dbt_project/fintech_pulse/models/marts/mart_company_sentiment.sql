with int_data as (
    select * from {{ ref('int_posts_enriched') }}
),

daily_sentiment as (
    select
        company,
        post_date,
        content_type,
        count(*) as mention_count,
        round(avg(sentiment_score), 3) as avg_sentiment,
        round(
            sum(sentiment_score * upvote_score) / nullif(sum(upvote_score), 0)
        , 3) as weighted_sentiment,
        sum(upvote_score) as total_upvotes,
        avg(upvote_velocity) as avg_velocity,
        count(case when sentiment_category = 'positive' then 1 end) as positive_count,
        count(case when sentiment_category = 'negative' then 1 end) as negative_count,
        count(case when sentiment_category = 'neutral' then 1 end) as neutral_count
    from int_data
    group by company, post_date, content_type
),

with_anomaly as (
    select
        *,
        avg(weighted_sentiment) over (
            partition by company
            order by post_date
            rows between 6 preceding and current row
        ) as rolling_7day_sentiment,
        case
            when weighted_sentiment < (
                avg(weighted_sentiment) over (
                    partition by company
                    order by post_date
                    rows between 6 preceding and current row
                ) * 0.8
            ) then true
            else false
        end as anomaly_flag
    from daily_sentiment
)

select * from with_anomaly