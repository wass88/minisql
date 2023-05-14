# [How Does a Database Work?](https://cstack.github.io/db_tutorial/) の Rust 実装

```
$ cargo run run.db
> insert 1 wass wass@example.com
Row { id: 1, name: wass, email: wass@example.com }
> insert 12 banana banana@example.com
Row { id: 12, name: banana, email: banana@example.com }
> insert 5 corn corn@example.coom
Row { id: 5, name: corn, email: corn@example.coom }
> .btree
Table { root_page_num: 0 }
Node 0 NodeType: Leaf, IsRoot: Yes, Parent: 0 ( NumCells: 3, NextLeaf 0 )
[1] Row { id: 1, name: wass, email: wass@example.com }
[5] Row { id: 5, name: corn, email: corn@example.coom }
[12] Row { id: 12, name: banana, email: banana@example.com }

> select 1
Row { id: 1, name: wass, email: wass@example.com }
```
