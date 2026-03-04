import { useState, useEffect, useRef, useCallback } from 'react'

// ============================================================
// Types
// ============================================================
interface User {
  userId: string
  username: string
  createdAt: number
}

interface Post {
  postId: string
  userId: string
  username: string
  title: string
  postText: string
  subreddit: string
  imageUrl: string
  imageCaption: string
  createdAt: number
  commentCount?: number
  replyCount?: number
}

interface Comment {
  commentId: string
  postId: string
  userId: string
  username: string
  commentText: string
  parentCommentId: string
  createdAt: number
}

interface Reply {
  replyId: string
  commentId: string
  postId: string
  generatedReply: string
  timestamp: number
}

interface BatchJob {
  jobId: string
  status: string
  message?: string
  filename?: string
  uploadedAt?: number
  totalRecords?: number
  processedRecords?: number
  sparkJobId?: string
}

interface Stats {
  users: number
  posts: number
  comments: number
  replies: number
  batchJobs: number
  wsClients: number
  kafkaConnected: boolean
}

interface Toast {
  id: string
  message: string
  type: 'success' | 'error' | 'info'
}

// ============================================================
// API Config
// ============================================================
// When served via nginx, API/WS requests are reverse-proxied
// to the API gateway automatically. Use relative paths.
// Falls back to absolute URLs for local dev (vite dev server).
const API_URL = import.meta.env.VITE_API_URL || ''
const WS_URL = import.meta.env.VITE_WS_URL || `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}`

function authHeaders(): Record<string, string> {
  const userId = localStorage.getItem('userId')
  const headers: Record<string, string> = {
    // Required to bypass ngrok's free-tier browser warning on API calls
    'ngrok-skip-browser-warning': 'true',
  }
  if (userId) headers['X-User-Id'] = userId
  return headers
}

async function api(path: string, options?: RequestInit) {
  const res = await fetch(`${API_URL}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...authHeaders(),
      ...options?.headers,
    },
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    const err = new Error(text || `${res.status} ${res.statusText}`)
    ;(err as any).status = res.status
    throw err
  }
  return res.json()
}

// ============================================================
// Main App
// ============================================================
export default function App() {
  const [user, setUser] = useState<User | null>(null)
  const [view, setView] = useState<'feed' | 'batch'>('feed')
  const [stats, setStats] = useState<Stats | null>(null)
  const [toasts, setToasts] = useState<Toast[]>([])
  const [wsConnected, setWsConnected] = useState(false)
  const [liveReplies, setLiveReplies] = useState<Reply[]>([])
  const wsRef = useRef<WebSocket | null>(null)

  // Toast helper
  const addToast = useCallback((message: string, type: Toast['type'] = 'info') => {
    const id = Date.now().toString()
    setToasts(prev => [...prev, { id, message, type }])
    setTimeout(() => setToasts(prev => prev.filter(t => t.id !== id)), 4000)
  }, [])

  // Check for saved session
  useEffect(() => {
    const savedUserId = localStorage.getItem('userId')
    const savedUsername = localStorage.getItem('username')
    if (savedUserId && savedUsername) {
      api('/api/auth/me').then(data => {
        setUser({ userId: data.userId, username: data.username, createdAt: data.createdAt })
      }).catch((err) => {
        // Server explicitly rejected the user (401) - clear stale session
        localStorage.removeItem('userId')
        localStorage.removeItem('username')
      })
    }
  }, [])

  // Fetch stats
  useEffect(() => {
    const fetchStats = async () => {
      try {
        const data = await api('/api/stats')
        setStats(data)
      } catch {
        // API not available yet
      }
    }
    fetchStats()
    const interval = setInterval(fetchStats, 5000)
    return () => clearInterval(interval)
  }, [])

  // WebSocket
  useEffect(() => {
    const connect = () => {
      const ws = new WebSocket(`${WS_URL}/ws/replies`)

      ws.onopen = () => {
        setWsConnected(true)
      }

      ws.onmessage = (event) => {
        try {
          const reply = JSON.parse(event.data) as Reply
          setLiveReplies(prev => [reply, ...prev].slice(0, 200))
        } catch (e) {
          console.error('Failed to parse WS message:', e)
        }
      }

      ws.onclose = () => {
        setWsConnected(false)
        setTimeout(connect, 5000)
      }

      ws.onerror = () => ws.close()
      wsRef.current = ws
    }

    connect()
    return () => wsRef.current?.close()
  }, [])

  const handleLogin = (loggedInUser: User) => {
    setUser(loggedInUser)
    localStorage.setItem('userId', loggedInUser.userId)
    localStorage.setItem('username', loggedInUser.username)
    addToast(`Welcome, ${loggedInUser.username}!`, 'success')
  }

  const handleLogout = () => {
    setUser(null)
    localStorage.removeItem('userId')
    localStorage.removeItem('username')
  }

  // Show login screen if not authenticated
  if (!user) {
    return (
      <>
        <LoginScreen onLogin={handleLogin} addToast={addToast} />
        <ToastContainer toasts={toasts} />
      </>
    )
  }

  return (
    <div className="app-shell">
      {/* Top Bar */}
      <header className="topbar">
        <div className="topbar-inner">
          <div className="topbar-left">
            <div className="logo-mark">R</div>
            <span className="logo-name">RedditAI</span>
          </div>

          <nav className="topbar-nav">
            <button
              className={`topbar-tab ${view === 'feed' ? 'active' : ''}`}
              onClick={() => setView('feed')}
            >
              Feed
            </button>
            <button
              className={`topbar-tab ${view === 'batch' ? 'active' : ''}`}
              onClick={() => setView('batch')}
            >
              Batch Upload
            </button>
          </nav>

          <div className="topbar-right">
            <div className="topbar-status">
              <span className={`status-dot ${wsConnected ? 'live' : 'off'}`} />
              <span className="status-label">{wsConnected ? 'Live' : 'Offline'}</span>
            </div>
            {stats && (
              <div className="topbar-stat">{stats.posts} posts / {stats.replies} AI replies</div>
            )}
            <div className="topbar-user">
              <div className="user-avatar">{user.username[0].toUpperCase()}</div>
              <span className="user-name">{user.username}</span>
              <button className="btn-logout" onClick={handleLogout} title="Log out">Sign out</button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="main-content">
        {view === 'feed' && (
          <FeedView user={user} liveReplies={liveReplies} addToast={addToast} />
        )}
        {view === 'batch' && (
          <BatchView user={user} addToast={addToast} />
        )}
      </main>

      <ToastContainer toasts={toasts} />
    </div>
  )
}

// ============================================================
// Toast Container
// ============================================================
function ToastContainer({ toasts }: { toasts: Toast[] }) {
  return (
    <div className="toast-container">
      {toasts.map(toast => (
        <div key={toast.id} className={`toast ${toast.type}`}>
          <span className="toast-icon">
            {toast.type === 'success' ? '\u2713' : toast.type === 'error' ? '\u2717' : 'i'}
          </span>
          {toast.message}
        </div>
      ))}
    </div>
  )
}

// ============================================================
// Login Screen
// ============================================================
function LoginScreen({ onLogin, addToast }: {
  onLogin: (user: User) => void
  addToast: (msg: string, type: Toast['type']) => void
}) {
  const [username, setUsername] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!username.trim() || username.trim().length < 2) return

    setLoading(true)
    try {
      const user = await api('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ username: username.trim() }),
      })
      onLogin(user)
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error'
      addToast(`Could not connect to server: ${msg}`, 'error')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="login-page">
      <div className="login-card">
        <div className="login-logo">
          <div className="logo-mark large">R</div>
        </div>
        <h1 className="login-title">RedditAI</h1>
        <p className="login-subtitle">Big Data Reply Generation Platform</p>

        <form onSubmit={handleSubmit} className="login-form">
          <input
            className="login-input"
            type="text"
            placeholder="Enter a username to continue"
            value={username}
            onChange={e => setUsername(e.target.value)}
            maxLength={30}
            autoFocus
          />
          <button
            className="login-btn"
            type="submit"
            disabled={loading || username.trim().length < 2}
          >
            {loading ? 'Connecting...' : 'Join'}
          </button>
        </form>

        <p className="login-hint">No password needed -- just pick a name.</p>

        <div className="login-info">
          <div className="login-info-row">
            <span className="info-tag">Kafka</span>
            <span className="info-tag">Spark</span>
            <span className="info-tag">HDFS</span>
            <span className="info-tag">Scala</span>
            <span className="info-tag">Groq LLM</span>
          </div>
        </div>
      </div>
    </div>
  )
}

// ============================================================
// Feed View
// ============================================================
function FeedView({ user, liveReplies, addToast }: {
  user: User
  liveReplies: Reply[]
  addToast: (msg: string, type: Toast['type']) => void
}) {
  const [posts, setPosts] = useState<Post[]>([])
  const [expandedPost, setExpandedPost] = useState<string | null>(null)
  const [newTitle, setNewTitle] = useState('')
  const [newBody, setNewBody] = useState('')
  const [newSubreddit, setNewSubreddit] = useState('general')
  const [posting, setPosting] = useState(false)
  const [showNewPost, setShowNewPost] = useState(false)
  const [imageFile, setImageFile] = useState<File | null>(null)
  const [imagePreview, setImagePreview] = useState<string | null>(null)
  const imageInputRef = useRef<HTMLInputElement>(null)

  // Load posts
  useEffect(() => {
    api('/api/posts').then(setPosts).catch(() => {})
    const interval = setInterval(() => {
      api('/api/posts').then(setPosts).catch(() => {})
    }, 8000)
    return () => clearInterval(interval)
  }, [])

  const handleImageSelect = (file: File | null) => {
    if (file && file.type.startsWith('image/')) {
      setImageFile(file)
      const reader = new FileReader()
      reader.onload = (e) => setImagePreview(e.target?.result as string)
      reader.readAsDataURL(file)
    } else {
      setImageFile(null)
      setImagePreview(null)
    }
  }

  const handleCreatePost = async () => {
    if (!newTitle.trim() || !newBody.trim()) return
    setPosting(true)
    try {
      let imageUrl = ''

      // Upload image first if one is selected
      if (imageFile) {
        const formData = new FormData()
        formData.append('file', imageFile)
        const uploadRes = await fetch(`${API_URL}/api/upload-image`, {
          method: 'POST',
          headers: authHeaders(),
          body: formData,
        })
        if (!uploadRes.ok) throw new Error('Image upload failed')
        const uploadData = await uploadRes.json()
        imageUrl = uploadData.imageUrl
      }

      const post = await api('/api/posts', {
        method: 'POST',
        body: JSON.stringify({
          title: newTitle.trim(),
          postText: newBody.trim(),
          subreddit: newSubreddit.trim() || 'general',
          imageUrl,
        }),
      })
      setPosts(prev => [post, ...prev])
      setNewTitle('')
      setNewBody('')
      setNewSubreddit('general')
      setImageFile(null)
      setImagePreview(null)
      setShowNewPost(false)
      addToast('Post created!', 'success')
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error'
      addToast(`Failed to create post: ${msg}`, 'error')
    } finally {
      setPosting(false)
    }
  }

  return (
    <div className="feed-layout">
      {/* Center Column: Posts Feed */}
      <div className="feed-center">
        {/* Create Post Button / Form */}
        {!showNewPost ? (
          <button className="new-post-trigger" onClick={() => setShowNewPost(true)}>
            <div className="user-avatar small">{user.username[0].toUpperCase()}</div>
            <span>What's on your mind, {user.username}?</span>
          </button>
        ) : (
          <div className="card compose-card">
            <div className="compose-header">
              <div className="user-avatar small">{user.username[0].toUpperCase()}</div>
              <span className="compose-user">{user.username}</span>
              <button className="btn-close" onClick={() => setShowNewPost(false)}>x</button>
            </div>
            <input
              className="compose-title"
              type="text"
              placeholder="Title"
              value={newTitle}
              onChange={e => setNewTitle(e.target.value)}
              autoFocus
            />
            <textarea
              className="compose-body"
              placeholder="Write something..."
              value={newBody}
              onChange={e => setNewBody(e.target.value)}
              rows={4}
            />

            {/* Image Upload */}
            <div className="compose-image-section">
              <input
                ref={imageInputRef}
                type="file"
                accept="image/jpeg,image/png,image/gif,image/webp"
                style={{ display: 'none' }}
                onChange={e => handleImageSelect(e.target.files?.[0] || null)}
              />
              {imagePreview ? (
                <div className="image-preview-container">
                  <img src={imagePreview} alt="Preview" className="image-preview" />
                  <button
                    className="btn-remove-image"
                    onClick={() => { setImageFile(null); setImagePreview(null) }}
                    title="Remove image"
                  >x</button>
                </div>
              ) : (
                <button
                  className="btn btn-secondary btn-sm"
                  onClick={() => imageInputRef.current?.click()}
                  type="button"
                >
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{marginRight: '6px', verticalAlign: 'middle'}}>
                    <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
                    <circle cx="8.5" cy="8.5" r="1.5"/>
                    <polyline points="21 15 16 10 5 21"/>
                  </svg>
                  Add Image
                </button>
              )}
            </div>

            <div className="compose-footer">
              <div className="compose-sub">
                <span className="sub-prefix">r/</span>
                <input
                  className="sub-input"
                  type="text"
                  placeholder="general"
                  value={newSubreddit}
                  onChange={e => setNewSubreddit(e.target.value)}
                />
              </div>
              <button
                className="btn btn-primary"
                onClick={handleCreatePost}
                disabled={posting || !newTitle.trim() || !newBody.trim()}
              >
                {posting ? (imageFile ? 'Uploading...' : 'Posting...') : 'Post'}
              </button>
            </div>
          </div>
        )}

        {/* Posts List */}
        {posts.length === 0 ? (
          <div className="empty-state">
            <div className="empty-icon">
              <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
              </svg>
            </div>
            <p>No posts yet. Be the first to share something!</p>
          </div>
        ) : (
          posts.map(post => (
            <PostCard
              key={post.postId}
              post={post}
              user={user}
              isExpanded={expandedPost === post.postId}
              onToggle={() => setExpandedPost(expandedPost === post.postId ? null : post.postId)}
              liveReplies={liveReplies}
              addToast={addToast}
            />
          ))
        )}
      </div>

      {/* Right Sidebar: Live AI Replies */}
      <aside className="feed-sidebar">
        <div className="card sidebar-card">
          <div className="sidebar-header">
            <h3>AI Replies (Live)</h3>
            <span className="live-badge">LIVE</span>
          </div>
          <div className="sidebar-feed">
            {liveReplies.length === 0 ? (
              <div className="sidebar-empty">
                <p>Waiting for AI replies...</p>
                <p className="hint">Comment on a post to trigger the pipeline:</p>
                <p className="hint">Kafka &rarr; Spark (Scala) &rarr; Groq LLM &rarr; here</p>
              </div>
            ) : (
              liveReplies.slice(0, 30).map(reply => (
                <div key={reply.replyId} className="sidebar-reply">
                  <div className="reply-tag">AI</div>
                  <p className="reply-text">{reply.generatedReply}</p>
                  <span className="reply-time">{new Date(reply.timestamp).toLocaleTimeString()}</span>
                </div>
              ))
            )}
          </div>
        </div>
      </aside>
    </div>
  )
}

// ============================================================
// Post Card
// ============================================================
function PostCard({ post, user, isExpanded, onToggle, liveReplies, addToast }: {
  post: Post
  user: User
  isExpanded: boolean
  onToggle: () => void
  liveReplies: Reply[]
  addToast: (msg: string, type: Toast['type']) => void
}) {
  const [comments, setComments] = useState<Comment[]>([])
  const [replies, setReplies] = useState<Reply[]>([])
  const [commentText, setCommentText] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [loadedComments, setLoadedComments] = useState(false)

  // Load comments and replies when expanded
  useEffect(() => {
    if (isExpanded && !loadedComments) {
      Promise.all([
        api(`/api/comments/${post.postId}`),
        api(`/api/replies/${post.postId}`),
      ]).then(([c, r]) => {
        setComments(c)
        setReplies(r)
        setLoadedComments(true)
      }).catch(() => {})
    }
  }, [isExpanded, loadedComments, post.postId])

  // Merge live replies for this post
  const postLiveReplies = liveReplies.filter(r => r.postId === post.postId)
  const allReplies = [...postLiveReplies, ...replies.filter(r => !postLiveReplies.find(lr => lr.replyId === r.replyId))]

  const handleComment = async () => {
    if (!commentText.trim()) return
    setSubmitting(true)
    try {
      const comment = await api('/api/comments', {
        method: 'POST',
        body: JSON.stringify({
          postId: post.postId,
          commentText: commentText.trim(),
        }),
      })
      setComments(prev => [...prev, comment])
      setCommentText('')
      addToast('Comment sent! AI reply coming via Kafka -> Spark -> LLM...', 'info')
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error'
      addToast(`Failed to send comment: ${msg}`, 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const timeAgo = (ts: number) => {
    const sec = Math.floor((Date.now() - ts) / 1000)
    if (sec < 60) return `${sec}s ago`
    if (sec < 3600) return `${Math.floor(sec / 60)}m ago`
    if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`
    return `${Math.floor(sec / 86400)}d ago`
  }

  return (
    <div className="card post-card">
      {/* Post Header */}
      <div className="post-header">
        <div className="user-avatar small">{(post.username || 'U')[0].toUpperCase()}</div>
        <div className="post-meta-info">
          <span className="post-author">{post.username}</span>
          <span className="post-dot">&middot;</span>
          <span className="post-sub">r/{post.subreddit}</span>
          <span className="post-dot">&middot;</span>
          <span className="post-time">{timeAgo(post.createdAt)}</span>
        </div>
      </div>

      {/* Post Content */}
      <h2 className="post-title">{post.title}</h2>

      {/* Post Image */}
      {post.imageUrl && (
        <div className="post-image-container">
          <img
            src={post.imageUrl.startsWith('http') ? post.imageUrl : `${API_URL}${post.imageUrl}`}
            alt={post.imageCaption || post.title}
            className="post-image"
            loading="lazy"
          />
          {post.imageCaption && (
            <p className="post-image-caption">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{marginRight: '4px', verticalAlign: 'middle', opacity: 0.6}}>
                <circle cx="12" cy="12" r="3"/>
                <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z"/>
              </svg>
              <span className="caption-label">AI Vision:</span> {post.imageCaption}
            </p>
          )}
        </div>
      )}

      <p className="post-body">{post.postText}</p>

      {/* Post Actions */}
      <div className="post-actions">
        <button className="action-btn" onClick={onToggle}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
          </svg>
          {(post.commentCount || 0)} Comments
        </button>
        <span className="action-stat">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="12" cy="12" r="3" />
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z" />
          </svg>
          {(post.replyCount || 0)} AI Replies
        </span>
      </div>

      {/* Expanded: Comments + Replies */}
      {isExpanded && (
        <div className="post-expanded">
          {/* Comment Input */}
          <div className="comment-input-row">
            <div className="user-avatar tiny">{user.username[0].toUpperCase()}</div>
            <input
              className="comment-input"
              type="text"
              placeholder="Write a comment..."
              value={commentText}
              onChange={e => setCommentText(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') handleComment() }}
            />
            <button
              className="btn btn-primary btn-sm"
              onClick={handleComment}
              disabled={submitting || !commentText.trim()}
            >
              {submitting ? '...' : 'Send'}
            </button>
          </div>

          {/* Comments Thread */}
          {comments.length === 0 && allReplies.length === 0 ? (
            <p className="no-comments">No comments yet. Start the conversation!</p>
          ) : (
            <div className="comments-thread">
              {comments.map(c => {
                const commentReplies = allReplies.filter(r => r.commentId === c.commentId)
                return (
                  <div key={c.commentId} className="comment-block">
                    <div className="comment-item">
                      <div className="user-avatar tiny">{(c.username || 'U')[0].toUpperCase()}</div>
                      <div className="comment-content">
                        <div className="comment-author">
                          {c.username}
                          <span className="comment-time">{timeAgo(c.createdAt)}</span>
                        </div>
                        <p className="comment-text">{c.commentText}</p>
                      </div>
                    </div>

                    {/* AI Replies to this comment */}
                    {commentReplies.map(r => (
                      <div key={r.replyId} className="ai-reply-item">
                        <div className="ai-avatar">AI</div>
                        <div className="comment-content">
                          <div className="comment-author ai-label">
                            RedditAI Bot
                            <span className="comment-time">{new Date(r.timestamp).toLocaleTimeString()}</span>
                          </div>
                          <p className="comment-text">{r.generatedReply}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                )
              })}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ============================================================
// Batch Upload View
// ============================================================
function BatchView({ user, addToast }: {
  user: User
  addToast: (msg: string, type: Toast['type']) => void
}) {
  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [jobs, setJobs] = useState<BatchJob[]>([])
  const [dragActive, setDragActive] = useState(false)
  const fileRef = useRef<HTMLInputElement>(null)

  // Load existing jobs
  useEffect(() => {
    api('/api/bulk/jobs').then(setJobs).catch(() => {})
  }, [])

  // Auto-poll active jobs every 3 seconds
  useEffect(() => {
    const activeJobs = jobs.filter(j =>
      ['uploading', 'uploaded', 'submitted', 'processing', 'inferring'].includes(j.status)
    )
    if (activeJobs.length === 0) return

    const interval = setInterval(async () => {
      for (const job of activeJobs) {
        try {
          const status = await api(`/api/bulk/status/${job.jobId}`)
          setJobs(prev => prev.map(j => j.jobId === job.jobId ? { ...j, ...status } : j))
        } catch {
          // ignore polling errors
        }
      }
    }, 3000)

    return () => clearInterval(interval)
  }, [jobs.map(j => `${j.jobId}:${j.status}`).join(',')])

  const handleUpload = async () => {
    if (!file) return
    setUploading(true)

    try {
      const formData = new FormData()
      formData.append('file', file)

      const res = await fetch(`${API_URL}/api/bulk/upload`, {
        method: 'POST',
        headers: authHeaders(),
        body: formData,
      })

      if (!res.ok) throw new Error('Upload failed')

      const job = await res.json()
      setJobs(prev => [job, ...prev])
      setFile(null)
      addToast(`Batch job submitted: ${job.jobId.slice(0, 8)}...`, 'success')
    } catch {
      addToast('Upload failed', 'error')
    } finally {
      setUploading(false)
    }
  }

  const checkStatus = async (jobId: string) => {
    try {
      const status = await api(`/api/bulk/status/${jobId}`)
      setJobs(prev => prev.map(j => j.jobId === jobId ? { ...j, ...status } : j))
      addToast(`Job ${jobId.slice(0, 8)}... status: ${status.status}`, 'info')
    } catch {
      addToast('Failed to check status', 'error')
    }
  }

  const downloadResults = async (jobId: string) => {
    try {
      const res = await fetch(`${API_URL}/api/bulk/download/${jobId}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error('Download not ready')
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `replies_${jobId}.json`
      a.click()
      URL.revokeObjectURL(url)
      addToast('Download started', 'success')
    } catch {
      addToast('Results not yet available', 'info')
    }
  }

  const getProgressPercent = (job: BatchJob): number => {
    if (job.status === 'completed') return 100
    if (!job.totalRecords || job.totalRecords === 0) return 0
    return Math.min(Math.round(((job.processedRecords || 0) / job.totalRecords) * 100), 99)
  }

  const getStatusLabel = (job: BatchJob): string => {
    if (job.status === 'inferring') {
      const pct = getProgressPercent(job)
      return `Inferring ${pct}%`
    }
    return job.status
  }

  const isActive = (status: string) =>
    ['uploading', 'uploaded', 'submitted', 'processing', 'inferring'].includes(status)

  return (
    <div className="batch-layout">
      <div className="batch-left">
        <h2 className="batch-heading">Batch Upload</h2>
        <p className="batch-desc">
          Upload a JSON file with posts and comments (up to 10k+ records).
          Data flows through HDFS, Spark Batch (Scala preprocessing), and the Groq LLM for AI reply generation.
        </p>

        {/* Upload Zone */}
        <div
          className={`upload-zone ${dragActive ? 'active' : ''} ${file ? 'has-file' : ''}`}
          onClick={() => fileRef.current?.click()}
          onDragOver={e => { e.preventDefault(); setDragActive(true) }}
          onDragLeave={() => setDragActive(false)}
          onDrop={e => {
            e.preventDefault()
            setDragActive(false)
            const f = e.dataTransfer.files[0]
            if (f?.name.endsWith('.json')) setFile(f)
          }}
        >
          <input
            ref={fileRef}
            type="file"
            accept=".json"
            style={{ display: 'none' }}
            onChange={e => setFile(e.target.files?.[0] || null)}
          />
          {file ? (
            <>
              <div className="upload-file-icon">
                <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
                  <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                  <polyline points="14 2 14 8 20 8" />
                </svg>
              </div>
              <div className="upload-text">{file.name}</div>
              <div className="upload-hint">{(file.size / 1024 / 1024).toFixed(2)} MB</div>
            </>
          ) : (
            <>
              <div className="upload-file-icon">
                <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                  <polyline points="17 8 12 3 7 8" />
                  <line x1="12" y1="3" x2="12" y2="15" />
                </svg>
              </div>
              <div className="upload-text">Drop a JSON file here, or click to browse</div>
              <div className="upload-hint">Supports bulk JSON with nested posts and comments</div>
            </>
          )}
        </div>

        {file && (
          <button
            className="btn btn-primary btn-block"
            onClick={handleUpload}
            disabled={uploading}
          >
            {uploading ? 'Uploading & Processing...' : 'Upload & Process'}
          </button>
        )}

        {/* Format Reference */}
        <div className="card format-card">
          <h3>Expected JSON Format</h3>
          <pre className="json-preview">{`[
  {
    "postId": "p1",
    "userId": "u1",
    "username": "user1",
    "title": "Post Title",
    "postText": "Content...",
    "subreddit": "technology",
    "timestamp": 1700000000000,
    "comments": [
      {
        "commentId": "c1",
        "userId": "u2",
        "username": "user2",
        "commentText": "Great post!",
        "timestamp": 1700000001000,
        "parentCommentId": null
      }
    ]
  }
]`}</pre>
        </div>
      </div>

      {/* Jobs List */}
      <div className="batch-right">
        <h3 className="jobs-heading">Processing Jobs</h3>
        {jobs.length === 0 ? (
          <div className="empty-state small">
            <p>No batch jobs yet. Upload a file to get started.</p>
          </div>
        ) : (
          <div className="jobs-list">
            {jobs.map(job => (
              <div key={job.jobId} className={`card job-card ${isActive(job.status) ? 'job-active' : ''}`}>
                <div className="job-top">
                  <span className="job-id">{job.jobId.slice(0, 12)}...</span>
                  <span className={`job-status ${job.status}`}>{getStatusLabel(job)}</span>
                </div>
                {job.filename && <p className="job-file">{job.filename}</p>}
                {job.uploadedAt && (
                  <p className="job-time">{new Date(job.uploadedAt).toLocaleString()}</p>
                )}

                {/* Progress Section */}
                {(job.totalRecords != null && job.totalRecords > 0) && (
                  <div className="job-progress-section">
                    <div className="job-progress-bar">
                      <div
                        className={`job-progress-fill ${job.status === 'completed' ? 'completed' : 'active'}`}
                        style={{ width: `${getProgressPercent(job)}%` }}
                      />
                    </div>
                    <div className="job-progress-info">
                      <span className="job-progress-counts">
                        {job.processedRecords || 0} / {job.totalRecords} records
                      </span>
                      <span className="job-progress-pct">
                        {getProgressPercent(job)}%
                      </span>
                    </div>
                  </div>
                )}

                {/* Active job indicator */}
                {isActive(job.status) && (
                  <div className="job-active-hint">Auto-refreshing...</div>
                )}

                <div className="job-actions">
                  <button className="btn btn-sm btn-secondary" onClick={() => checkStatus(job.jobId)}>
                    Refresh
                  </button>
                  {job.status === 'completed' && (
                    <button className="btn btn-sm btn-primary" onClick={() => downloadResults(job.jobId)}>
                      Download
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Pipeline Info */}
        <div className="card pipeline-card">
          <h3>Processing Pipeline</h3>
          <div className="pipeline-steps">
            <div className="pipeline-step">
              <span className="step-num">1</span>
              <div>
                <strong>Upload to HDFS</strong>
                <p>Raw JSON stored in distributed filesystem</p>
              </div>
            </div>
            <div className="pipeline-step">
              <span className="step-num">2</span>
              <div>
                <strong>Spark Batch (Scala)</strong>
                <p>Schema validation, text cleaning, deduplication</p>
              </div>
            </div>
            <div className="pipeline-step">
              <span className="step-num">3</span>
              <div>
                <strong>Groq LLM Inference</strong>
                <p>AI reply generation for each comment</p>
              </div>
            </div>
            <div className="pipeline-step">
              <span className="step-num">4</span>
              <div>
                <strong>HDFS Output</strong>
                <p>Enriched JSON with AI replies ready to download</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
