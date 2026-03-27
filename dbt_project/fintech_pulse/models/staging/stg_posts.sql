with source as (
    select * from raw_posts
),

cleaned as (
    select
        post_id,
        lower(company) as company,
        lower(subreddit) as subreddit,
        trim(title) as title,
        trim(body) as body,
        upvote_score,
        upvote_ratio,
        comment_count,
        sentiment_score,
        created_utc,
        loaded_at,
        updated_at,
        content_type
    from source
    where post_id is not null
        and sentiment_score is not null
        and body != ''
)

select * from cleaned