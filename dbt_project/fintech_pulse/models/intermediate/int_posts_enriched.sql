with stg as (
    select * from {{ ref('stg_posts') }}
),

enriched as (
    select
        post_id,
        company,
        subreddit,
        title,
        body,
        upvote_score,
        upvote_ratio,
        comment_count,
        sentiment_score,
        created_utc,
        content_type,

        -- Sentiment category
        case
            when sentiment_score >= 0.05 then 'positive'
            when sentiment_score <= -0.05 then 'negative'
            else 'neutral'
        end as sentiment_category,

        -- Hours since posted
        datediff('hour', created_utc, current_timestamp) as hours_since_posted,

        -- Upvote velocity (upvotes per hour)
        case
            when datediff('hour', created_utc, current_timestamp) > 0
            then round(upvote_score / datediff('hour', created_utc, current_timestamp), 2)
            else upvote_score
        end as upvote_velocity,

        -- Date fields for aggregation
        date_trunc('day', created_utc) as post_date,
        date_trunc('week', created_utc) as post_week

    from stg
)

select * from enriched