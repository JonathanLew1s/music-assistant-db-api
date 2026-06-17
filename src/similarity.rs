use std::sync::Arc;
use parking_lot::RwLock;

pub struct SimilarityIndex {
    vectors: Arc<RwLock<Vec<(i64, Vec<f32>)>>>,
}

impl SimilarityIndex {
    pub fn new(vectors: Vec<(i64, Vec<f32>)>) -> Self {
        tracing::info!("similarity index loaded: {} vectors", vectors.len());
        Self { vectors: Arc::new(RwLock::new(vectors)) }
    }

    pub fn empty() -> Self {
        Self { vectors: Arc::new(RwLock::new(vec![])) }
    }

    pub fn len(&self) -> usize {
        self.vectors.read().len()
    }

    pub fn reload(&self, vectors: Vec<(i64, Vec<f32>)>) {
        tracing::info!("similarity index reloaded: {} vectors", vectors.len());
        *self.vectors.write() = vectors;
    }

    pub fn find_similar(&self, query_id: i64, limit: usize, exclude_ids: &[i64]) -> Vec<(i64, f32)> {
        let vecs = self.vectors.read();
        let query_vec = match vecs.iter().find(|(id, _)| *id == query_id) {
            Some((_, v)) => v,
            None => return vec![],
        };
        let query_norm = l2_norm(query_vec);
        if query_norm == 0.0 {
            return vec![];
        }

        let mut scores: Vec<(i64, f32)> = vecs
            .iter()
            .filter(|(id, _)| *id != query_id && !exclude_ids.contains(id))
            .map(|(id, v)| {
                let score = cosine_similarity(query_vec, v, query_norm);
                (*id, score)
            })
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(limit);
        scores
    }
}

fn l2_norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

fn cosine_similarity(a: &[f32], b: &[f32], a_norm: f32) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let b_norm = l2_norm(b);
    if b_norm == 0.0 { return 0.0; }
    dot / (a_norm * b_norm)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index() -> SimilarityIndex {
        SimilarityIndex::new(vec![
            (1, vec![1.0, 0.0, 0.0]),
            (2, vec![1.0, 0.0, 0.0]),
            (3, vec![0.0, 1.0, 0.0]),
            (4, vec![-1.0, 0.0, 0.0]),
        ])
    }

    #[test]
    fn similar_to_identical() {
        let idx = make_index();
        let results = idx.find_similar(1, 3, &[]);
        assert_eq!(results[0].0, 2);
        assert!((results[0].1 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn opposite_is_least_similar() {
        let idx = make_index();
        let results = idx.find_similar(1, 3, &[]);
        let last = results.last().unwrap();
        assert_eq!(last.0, 4);
        assert!((last.1 + 1.0).abs() < 1e-6);
    }

    #[test]
    fn exclude_ids_respected() {
        let idx = make_index();
        let results = idx.find_similar(1, 3, &[2]);
        assert!(!results.iter().any(|(id, _)| *id == 2));
    }

    #[test]
    fn unknown_query_returns_empty() {
        let idx = make_index();
        assert!(idx.find_similar(999, 10, &[]).is_empty());
    }
}
