--find the number of distinct txn_from address and txn_to address per wallet 
SELECT count(distinct txn_from) as from_address, count(distinct txn_to) as to_address, address
 FROM `datapipeline-325719.etherscan.token_txn_by_address` 
 group by address


--find most frequent txn_from address, and the transaction count & sum inflow 
--0x6781a24ccc819941b7a4c9cc7b0dc32666e587e4 15 times 
-- 0x4cadde3de133ccb22718ff3ac3b54b86760895dd 2 times
-- 0x9d6a86facdcf24859a38e6b9a2ef87610a4fc157 1 time
select c.address, c.txn_from, c.txn_from_count, c.sum_inflow
from
(select address, txn_from, count(*) as txn_from_count, sum(inflow) as sum_inflow
 from `datapipeline-325719.etherscan.token_txn_by_address` 
 where address !=txn_from
group by address, txn_from 
order by address, count(*) desc) c
join(
select a.address, max(a.txn_from_count) as max_count from (
 select address, txn_from, count(*) as txn_from_count
 from `datapipeline-325719.etherscan.token_txn_by_address` 
 where address !=txn_from
group by address, txn_from 
order by address, count(*) desc) a
group by a.address) b on c.address=b.address and c.txn_from_count =b.max_count

--find most frequent txn_to address, and the transaction count & sum outflow 
--more scattered view, but some share the same outflow address or patterns 
--0xe9e11963f61322299f9919ff1dda01a825e82dbc

select c.address, c.txn_to, c.txn_to_count, c.sum_ouflow
from
(select address, txn_to, count(*) as txn_to_count, sum(ouflow) as sum_ouflow
 from `datapipeline-325719.etherscan.token_txn_by_address` 
 where address !=txn_to
group by address, txn_to 
order by address, count(*) desc) c
join(
select a.address, max(a.txn_to_count) as max_count from (
 select address, txn_to, count(*) as txn_to_count
 from `datapipeline-325719.etherscan.token_txn_by_address` 
 where address !=txn_to
group by address, txn_to 
order by address, count(*) desc) a
group by a.address) b on c.address=b.address and c.txn_to_count =b.max_count
order by txn_to

--txn_to addresses
 select address, txn_to, count(*) as txn_to_count
 from `datapipeline-325719.etherscan.token_txn_by_address` 
 where address !=txn_to
group by address, txn_to
